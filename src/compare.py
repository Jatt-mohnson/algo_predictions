"""Join Underdog, DraftKings, and Pinnacle into a single comparison view.

DraftKings and Pinnacle columns are output as vig-free implied probabilities
(0–100 scale) so they are directly comparable.  Underdog columns remain as
payout multipliers since their implied probability depends on the full entry
structure (handled by ud-picks).

When sources post a prop at different lines (e.g. DK at 15.5, UD at 14.5),
a Poisson fallback adjusts DK/Pinnacle probabilities to the base book's line
so the comparison is still meaningful.  Adjusted rows are flagged with a
boolean *_adj column.

Output: combined_odds.csv

Usage:
  uv run compare                      # default base: underdog (left join)
  uv run compare --base draftkings    # DraftKings rows drive the output
  uv run compare --base pinnacle
  uv run compare --base none          # outer join — all rows from all sources
  uv run compare --no-adjust          # disable Poisson threshold adjustment
"""

import argparse
import os
import sys

import pandas as pd

from src.common import (
    DRAFTKINGS_CSV,
    PINNACLE_CSV,
    UNDERDOG_CSV,
    COMBINED_CSV,
    normalize_player_name,
    adjust_prob_for_threshold,
)


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------

def load_sportsbook_probs(path: str, label: str) -> pd.DataFrame:
    """Load a DK or Pinnacle CSV and return vig-free implied probabilities.

    Returns columns: player, stat, threshold, {label}_over_prob, {label}_under_prob
    Probabilities are on a 0–100 scale.
    """
    if not os.path.exists(path):
        return pd.DataFrame()
    df = pd.read_csv(path)
    if "odds_decimal" not in df.columns:
        return pd.DataFrame()

    over = df[df["choice"] == "over"][["full_name", "stat_name", "stat_value", "odds_decimal"]].copy()
    under = df[df["choice"] == "under"][["full_name", "stat_name", "stat_value", "odds_decimal"]].copy()
    over = over.rename(columns={"odds_decimal": "over_odds"})
    under = under.rename(columns={"odds_decimal": "under_odds"})

    merged = over.merge(under, on=["full_name", "stat_name", "stat_value"], how="inner")
    merged = merged[
        merged["over_odds"].notna() & merged["under_odds"].notna() &
        (merged["over_odds"] > 0) & (merged["under_odds"] > 0)
    ]

    raw_over = 1.0 / merged["over_odds"]
    raw_under = 1.0 / merged["under_odds"]
    overround = raw_over + raw_under
    merged[f"{label}_over_prob"] = (raw_over / overround * 100).round(1)
    merged[f"{label}_under_prob"] = (raw_under / overround * 100).round(1)

    merged = merged.rename(columns={
        "full_name": "player",
        "stat_name": "stat",
        "stat_value": "threshold",
    })
    return merged[["player", "stat", "threshold", f"{label}_over_prob", f"{label}_under_prob"]]


def load_underdog_mults(path: str) -> pd.DataFrame:
    """Load the Underdog CSV and return per-side payout multipliers.

    Returns columns: player, stat, threshold, ud_over_mult, ud_under_mult
    """
    if not os.path.exists(path):
        return pd.DataFrame()
    df = pd.read_csv(path)
    if "payout_multiplier" not in df.columns:
        return pd.DataFrame()

    over = df[df["choice"] == "over"][["full_name", "stat_name", "stat_value", "payout_multiplier"]].copy()
    under = df[df["choice"] == "under"][["full_name", "stat_name", "stat_value", "payout_multiplier"]].copy()
    over = over.rename(columns={"payout_multiplier": "ud_over_mult"})
    under = under.rename(columns={"payout_multiplier": "ud_under_mult"})

    merged = over.merge(under, on=["full_name", "stat_name", "stat_value"], how="outer")
    merged = merged.rename(columns={
        "full_name": "player",
        "stat_name": "stat",
        "stat_value": "threshold",
    })
    return merged[["player", "stat", "threshold", "ud_over_mult", "ud_under_mult"]]


def _join_keys(df: pd.DataFrame) -> pd.DataFrame:
    """Add normalised _jp / _js columns used for matching."""
    df = df.copy()
    df["_jp"] = df["player"].apply(normalize_player_name)
    df["_js"] = df["stat"].str.lower().str.strip()
    return df


# ---------------------------------------------------------------------------
# Two-pass join: exact match then Poisson-adjusted fuzzy fallback
# ---------------------------------------------------------------------------

def _exact_merge(base: pd.DataFrame, book: pd.DataFrame,
                 cols: list[str], how: str) -> pd.DataFrame:
    """Merge base with book on normalised player/stat/threshold."""
    base = _join_keys(base)
    book = _join_keys(book)

    result = base.merge(
        book[["_jp", "_js", "threshold"] + cols],
        on=["_jp", "_js", "threshold"],
        how=how,
    )
    # Outer merges split player/stat into _x/_y — coalesce them.
    if "player_x" in result.columns:
        result["player"] = result["player_x"].combine_first(result["player_y"])
        result["stat"] = result["stat_x"].combine_first(result["stat_y"])
        result = result.drop(columns=["player_x", "player_y", "stat_x", "stat_y"])
    return result


def _fuzzy_fill_probs(combined: pd.DataFrame, book: pd.DataFrame,
                      over_col: str, under_col: str, adj_col: str,
                      max_diff: float = 1.0) -> pd.DataFrame:
    """For rows where over_col is still NaN after the exact merge, search book
    for a nearby threshold (within max_diff) and fill using Poisson-adjusted
    probabilities.  Marks adjusted rows with adj_col = True.
    """
    if book.empty:
        return combined

    book = _join_keys(book)
    # Build a dict (jp, js) → DataFrame of available rows for O(1) lookup
    book_by_key: dict[tuple, pd.DataFrame] = {}
    for (jp, js), grp in book.groupby(["_jp", "_js"]):
        book_by_key[(jp, js)] = grp

    combined = combined.copy()
    if adj_col not in combined.columns:
        combined[adj_col] = False

    unmatched_idx = combined.index[combined[over_col].isna()]
    for idx in unmatched_idx:
        row = combined.loc[idx]
        key = (row["_jp"], row["_js"])
        target = float(row["threshold"])

        grp = book_by_key.get(key)
        if grp is None:
            continue

        grp = grp.copy()
        grp["_diff"] = (grp["threshold"].astype(float) - target).abs()
        close = grp[grp["_diff"] <= max_diff]
        if close.empty:
            continue

        best = close.loc[close["_diff"].idxmin()]
        from_line = float(best["threshold"])
        raw_over = float(best[over_col]) / 100.0   # convert % → 0-1

        adj_over = adjust_prob_for_threshold(raw_over, from_line, target)
        if adj_over is None:
            continue

        combined.loc[idx, over_col] = round(adj_over * 100, 1)
        combined.loc[idx, under_col] = round((1.0 - adj_over) * 100, 1)
        combined.loc[idx, adj_col] = True

    return combined


def _fuzzy_fill_mults(combined: pd.DataFrame, book: pd.DataFrame,
                      over_col: str, under_col: str, adj_col: str,
                      max_diff: float = 1.0) -> pd.DataFrame:
    """Nearest-threshold fallback for Underdog multipliers when UD is not the base.

    Multipliers are not probabilities so no Poisson adjustment is applied —
    we simply pull the closest available line's multipliers.
    """
    if book.empty:
        return combined

    book = _join_keys(book)
    book_by_key: dict[tuple, pd.DataFrame] = {}
    for (jp, js), grp in book.groupby(["_jp", "_js"]):
        book_by_key[(jp, js)] = grp

    combined = combined.copy()
    if adj_col not in combined.columns:
        combined[adj_col] = False

    unmatched_idx = combined.index[combined[over_col].isna()]
    for idx in unmatched_idx:
        row = combined.loc[idx]
        key = (row["_jp"], row["_js"])
        target = float(row["threshold"])

        grp = book_by_key.get(key)
        if grp is None:
            continue

        grp = grp.copy()
        grp["_diff"] = (grp["threshold"].astype(float) - target).abs()
        close = grp[grp["_diff"] <= max_diff]
        if close.empty:
            continue

        best = close.loc[close["_diff"].idxmin()]
        combined.loc[idx, over_col] = best[over_col]
        combined.loc[idx, under_col] = best[under_col]
        combined.loc[idx, adj_col] = True

    return combined


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Compare vig-free implied probabilities across Underdog, DraftKings, and Pinnacle.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
DK and Pinnacle columns show vig-free implied probabilities (0-100).
Underdog columns show payout multipliers (use ud-picks for UD edge analysis).
Rows where a book's line differs from the base by ≤1 unit are Poisson-adjusted
and flagged with a *_adj = True column.
        """,
    )
    parser.add_argument(
        "--base",
        choices=["underdog", "draftkings", "pinnacle", "none"],
        default="underdog",
        help="Which book drives the rows. 'none' = outer join of all sources. (default: underdog)",
    )
    parser.add_argument(
        "--no-adjust",
        action="store_true",
        help="Disable Poisson threshold adjustment for ±1 line differences.",
    )
    args = parser.parse_args()

    dk = load_sportsbook_probs(DRAFTKINGS_CSV, "dk")
    pinn = load_sportsbook_probs(PINNACLE_CSV, "pinn")
    ud = load_underdog_mults(UNDERDOG_CSV)

    book_map = {"draftkings": dk, "pinnacle": pinn, "underdog": ud}

    if all(df.empty for df in book_map.values()):
        print("No data found. Run draftkings, pinnacle, and/or underdog scripts first.")
        sys.exit(1)

    use_outer = args.base == "none"
    join_how = "outer" if use_outer else "left"
    max_diff = 0.0 if args.no_adjust else 1.0

    # Build the combined table starting from the base
    if use_outer:
        print("Base: none (outer join — all rows from all sources)")
        base_df = next(df.copy() for df in book_map.values() if not df.empty)
        others = [(name, df) for name, df in book_map.items() if df is not base_df]
    else:
        base_name = args.base
        if book_map[base_name].empty:
            print(f"No data for '{base_name}'. Run the corresponding fetch script first.")
            sys.exit(1)
        print(f"Base: {base_name} ({join_how} join{'' if args.no_adjust else ', Poisson adjustment enabled'})")
        base_df = book_map[base_name].copy()
        others = [(name, df) for name, df in book_map.items() if name != base_name]

    # Add join keys to the combined df (kept until the end for fuzzy fill)
    combined = _join_keys(base_df)

    for name, book_df in others:
        if book_df.empty:
            continue

        if name == "underdog":
            cols = ["ud_over_mult", "ud_under_mult"]
        else:
            label = "dk" if name == "draftkings" else "pinn"
            cols = [f"{label}_over_prob", f"{label}_under_prob"]

        # Pass 1: exact threshold match
        combined = _exact_merge(combined, book_df, cols, how=join_how)
        # Re-add join keys that may be dropped by the merge
        combined = _join_keys(combined)

        # Pass 2: Poisson-adjusted fuzzy fallback for unmatched rows
        if not args.no_adjust and not use_outer:
            adj_col = f"{name[:2] if name != 'underdog' else 'ud'}_adj"
            if name == "underdog":
                combined = _fuzzy_fill_mults(
                    combined, book_df,
                    "ud_over_mult", "ud_under_mult", "ud_adj",
                    max_diff=max_diff,
                )
            else:
                label = "dk" if name == "draftkings" else "pinn"
                combined = _fuzzy_fill_probs(
                    combined, book_df,
                    f"{label}_over_prob", f"{label}_under_prob", f"{label}_adj",
                    max_diff=max_diff,
                )

    # Drop internal join keys
    combined = combined.drop(columns=["_jp", "_js"], errors="ignore")

    # Order columns
    desired_order = [
        "player", "stat", "threshold",
        "dk_over_prob", "dk_under_prob", "dk_adj",
        "pinn_over_prob", "pinn_under_prob", "pinn_adj",
        "ud_over_mult", "ud_under_mult", "ud_adj",
    ]
    cols = [c for c in desired_order if c in combined.columns]
    combined = combined[cols].sort_values(["player", "stat", "threshold"]).reset_index(drop=True)

    combined.to_csv(COMBINED_CSV, index=False)
    print(f"Wrote {len(combined)} rows to {COMBINED_CSV}")
    print(f"Columns: {', '.join(combined.columns)}")

    total = len(combined)
    for col, label in [
        ("dk_over_prob", "DraftKings"),
        ("pinn_over_prob", "Pinnacle"),
        ("ud_over_mult", "Underdog"),
    ]:
        if col not in combined.columns:
            continue
        matched = combined[col].notna().sum()
        adj_col = col.split("_")[0] + "_adj"
        adj = int(combined[adj_col].sum()) if adj_col in combined.columns else 0
        note = f", {adj} Poisson-adjusted" if adj else ""
        print(f"  {label}: {matched}/{total} matched{note}")


if __name__ == "__main__":
    main()
