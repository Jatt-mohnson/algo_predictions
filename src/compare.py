"""Join Underdog, DraftKings, and Pinnacle into a single comparison view.

Reads CSV files from Underdog, DraftKings, and Pinnacle and produces a unified
table showing each player/stat/threshold with odds from every source.

Output: combined_odds.csv

Usage:
  uv run compare                      # default base: underdog (left join)
  uv run compare --base draftkings    # DraftKings rows drive the output
  uv run compare --base pinnacle
  uv run compare --base none          # outer join — all rows from all sources
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
)

BOOKS = {
    "underdog": (UNDERDOG_CSV, "ud"),
    "draftkings": (DRAFTKINGS_CSV, "dk"),
    "pinnacle": (PINNACLE_CSV, "pinn"),
}


def load_sportsbook(path, label):
    """Load a sportsbook CSV and pivot over/under into columns."""
    if not os.path.exists(path):
        return pd.DataFrame()
    df = pd.read_csv(path)

    # Underdog uses payout_multiplier; DraftKings/Pinnacle use odds_decimal
    odds_col = "payout_multiplier" if "payout_multiplier" in df.columns else "odds_decimal"

    # Pivot so each (player, stat, threshold) has over and under columns
    over = df[df["choice"] == "over"][["full_name", "stat_name", "stat_value", odds_col]].copy()
    under = df[df["choice"] == "under"][["full_name", "stat_name", "stat_value", odds_col]].copy()

    over = over.rename(columns={odds_col: f"{label}_over"})
    under = under.rename(columns={odds_col: f"{label}_under"})

    merged = over.merge(under, on=["full_name", "stat_name", "stat_value"], how="outer")
    merged = merged.rename(columns={
        "full_name": "player",
        "stat_name": "stat",
        "stat_value": "threshold",
    })
    return merged


def main():
    parser = argparse.ArgumentParser(description="Compare odds across Underdog, DraftKings, and Pinnacle.")
    parser.add_argument(
        "--base",
        choices=["underdog", "draftkings", "pinnacle", "none"],
        default="underdog",
        help="Which book drives the rows (left join). Use 'none' for an outer join of all sources. (default: underdog)",
    )
    args = parser.parse_args()

    loaded = {name: load_sportsbook(path, label) for name, (path, label) in BOOKS.items()}

    if all(df.empty for df in loaded.values()):
        print("No data found. Run draftkings, pinnacle, and/or underdog scripts first.")
        sys.exit(1)

    use_outer = args.base == "none"
    join_how = "outer" if use_outer else "left"

    if not use_outer:
        base_name = args.base
        if loaded[base_name].empty:
            print(f"No data for base book '{base_name}'. Run the corresponding fetch script first.")
            sys.exit(1)
        print(f"Base: {base_name} ({join_how} join)")
        combined = loaded[base_name].copy()
    else:
        print("Base: none (outer join — all rows from all sources)")
        # Seed with first non-empty source
        combined = next(df.copy() for df in loaded.values() if not df.empty)

    for name, df in loaded.items():
        if not use_outer and name == args.base:
            continue
        if df.empty:
            continue

        df = df.copy()
        df["_join_player"] = df["player"].apply(normalize_player_name)
        combined["_join_player"] = combined["player"].apply(normalize_player_name)
        df["_join_stat"] = df["stat"].str.lower().str.strip()
        combined["_join_stat"] = combined["stat"].str.lower().str.strip()

        combined = combined.merge(
            df.drop(columns=["player", "stat"]),
            left_on=["_join_player", "_join_stat", "threshold"],
            right_on=["_join_player", "_join_stat", "threshold"],
            how=join_how,
        )
        combined = combined.drop(columns=["_join_player", "_join_stat"], errors="ignore")

        # Consolidate player/stat columns produced by outer joins
        if "player_x" in combined.columns:
            combined["player"] = combined["player_x"].combine_first(combined["player_y"])
            combined["stat"] = combined["stat_x"].combine_first(combined["stat_y"])
            combined = combined.drop(columns=["player_x", "player_y", "stat_x", "stat_y"])

    # Order columns nicely
    desired_order = [
        "player", "stat", "threshold",
        "dk_over", "dk_under",
        "pinn_over", "pinn_under",
        "ud_over", "ud_under",
    ]
    cols = [c for c in desired_order if c in combined.columns]
    combined = combined[cols].sort_values(["player", "stat", "threshold"]).reset_index(drop=True)

    combined.to_csv(COMBINED_CSV, index=False)
    print(f"Wrote {len(combined)} rows to {COMBINED_CSV}")
    print(f"Columns: {', '.join(combined.columns)}")

    # Show summary of data coverage
    total = len(combined)
    for col, label in [("dk_over", "DraftKings"), ("pinn_over", "Pinnacle"), ("ud_over", "Underdog")]:
        if col in combined.columns:
            matched = combined[col].notna().sum()
            print(f"  {label} matched: {matched}/{total}")


if __name__ == "__main__":
    main()
