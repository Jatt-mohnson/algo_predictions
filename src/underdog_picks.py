"""Find Underdog Fantasy picks using Kalshi as a price oracle.

Reads Kalshi and Underdog CSVs, joins on player/stat/threshold,
and surfaces legs where Kalshi implies a high enough probability to
make the Underdog entry +EV.

Each leg on Underdog has its own payout multiplier (and the over/under
of the same line can pay differently). The break-even probability for a
specific leg accounts for both the entry payout and that leg's multiplier:

  base_be     = (1 / total_payout) ^ (1 / legs)  × 100
  required_prob = base_be / ud_multiplier

A leg is +EV when: kalshi_prob > required_prob

Example — 1.5 3PM line, 2-leg entry at 3x base:
  over  (0.75x):  required = 57.7 / 0.75 = 76.9%   ← needs very high Kalshi probability
  under (1.1x):   required = 57.7 / 1.10 = 52.5%   ← needs less, higher multiplier rewards it
"""

import argparse
import os
import sys

import pandas as pd

from src.common import (
    SERIES_TO_STAT,
    KALSHI_CSV,
    UNDERDOG_CSV,
    DATA_DIR,
    parse_kalshi_title,
)

UNDERDOG_PICKS_CSV = os.path.join(DATA_DIR, "underdog_picks.csv")


def base_breakeven(legs: int, payout: float) -> float:
    """Base break-even probability (0-100) assuming a 1.0x leg multiplier."""
    return ((1.0 / payout) ** (1.0 / legs)) * 100


def required_prob(base_be: float, ud_multiplier: float) -> float:
    """Adjusted break-even for a specific leg given its Underdog payout multiplier.

    Higher multiplier → lower required probability (the leg is paying you more,
    so you need to be right less often to break even).
    """
    return base_be / ud_multiplier


def load_kalshi(path: str = KALSHI_CSV) -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame()
    df = pd.read_csv(path)
    rows = []
    for _, row in df.iterrows():
        player, threshold = parse_kalshi_title(row["title"])
        if player is None:
            continue
        series = row.get("series_ticker", "")
        if not series or (isinstance(series, float) and pd.isna(series)):
            series = row["ticker"].split("-")[0]
        stat = SERIES_TO_STAT.get(series)
        if not stat:
            continue
        yes_bid = row.get("yes_bid")
        yes_ask = row.get("yes_ask")
        no_bid = row.get("no_bid")
        no_ask = row.get("no_ask")
        yes_prob = (yes_bid + yes_ask) / 2 if pd.notna(yes_bid) and pd.notna(yes_ask) else None
        no_prob = (no_bid + no_ask) / 2 if pd.notna(no_bid) and pd.notna(no_ask) else None
        rows.append({
            "player": player,
            "stat": stat,
            "threshold": threshold,
            "ticker": row["ticker"],
            "kalshi_yes_prob": round(yes_prob, 1) if yes_prob is not None else None,
            "kalshi_no_prob": round(no_prob, 1) if no_prob is not None else None,
        })
    return pd.DataFrame(rows)


def load_underdog(path: str = UNDERDOG_CSV) -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame()
    return pd.read_csv(path)


def find_picks(legs: int, payout: float, min_edge: float = 0.0) -> pd.DataFrame:
    """Return Underdog legs where Kalshi probability exceeds the leg-adjusted break-even."""
    kalshi = load_kalshi()
    if kalshi.empty:
        print("No Kalshi data. Run `uv run kalshi` first (or use --refresh).")
        sys.exit(1)

    underdog = load_underdog()
    if underdog.empty:
        print("No Underdog data. Run `uv run underdog` first (or use --refresh).")
        sys.exit(1)

    base_be = base_breakeven(legs, payout)

    # Underdog stat_value is N-0.5; Kalshi threshold is N (N+ market)
    ud = underdog.copy()
    ud["threshold"] = (ud["stat_value"] + 0.5).round(0).astype(int)
    ud["_join_player"] = ud["full_name"].str.lower().str.strip()
    ud["_join_stat"] = ud["stat_name"].str.lower().str.strip()

    kalshi["_join_player"] = kalshi["player"].str.lower().str.strip()
    kalshi["_join_stat"] = kalshi["stat"].str.lower().str.strip()

    # Pivot Underdog: one row per (player, stat, threshold) with separate over/under multipliers
    over = (
        ud[ud["choice"] == "over"][["_join_player", "_join_stat", "threshold", "payout_multiplier"]]
        .rename(columns={"payout_multiplier": "ud_over_mult"})
    )
    under = (
        ud[ud["choice"] == "under"][["_join_player", "_join_stat", "threshold", "payout_multiplier"]]
        .rename(columns={"payout_multiplier": "ud_under_mult"})
    )
    ud_pivot = over.merge(under, on=["_join_player", "_join_stat", "threshold"], how="outer")

    joined = kalshi.merge(ud_pivot, on=["_join_player", "_join_stat", "threshold"], how="inner")
    joined = joined.drop(columns=["_join_player", "_join_stat"])

    picks = []
    for _, row in joined.iterrows():
        threshold_label = f"{int(row['threshold'])}+"

        # Over pick: Kalshi "yes" = player hits N+ = Underdog "over"
        yes_prob = row.get("kalshi_yes_prob")
        over_mult = row.get("ud_over_mult")
        if pd.notna(yes_prob) and pd.notna(over_mult) and over_mult > 0:
            req = required_prob(base_be, over_mult)
            picks.append({
                "player": row["player"],
                "stat": row["stat"],
                "threshold": threshold_label,
                "ud_pick": "over",
                "ud_mult": round(over_mult, 3),
                "kalshi_prob": yes_prob,
                "required_prob": round(req, 1),
                "edge": round(yes_prob - req, 1),
                "ticker": row["ticker"],
            })

        # Under pick: Kalshi "no" = player stays under N = Underdog "under"
        no_prob = row.get("kalshi_no_prob")
        under_mult = row.get("ud_under_mult")
        if pd.notna(no_prob) and pd.notna(under_mult) and under_mult > 0:
            req = required_prob(base_be, under_mult)
            picks.append({
                "player": row["player"],
                "stat": row["stat"],
                "threshold": threshold_label,
                "ud_pick": "under",
                "ud_mult": round(under_mult, 3),
                "kalshi_prob": no_prob,
                "required_prob": round(req, 1),
                "edge": round(no_prob - req, 1),
                "ticker": row["ticker"],
            })

    if not picks:
        return pd.DataFrame()

    df = pd.DataFrame(picks)
    df = df[df["edge"] >= min_edge].sort_values("edge", ascending=False).reset_index(drop=True)
    return df


def refresh_data():
    from src.main import get_client, fetch_nba_player_props
    from src.underdog import fetch_underdog_data, parse_nba_props

    print("Refreshing Kalshi NBA player props...")
    client = get_client()
    kalshi_df = fetch_nba_player_props(client)
    kalshi_df.to_csv(KALSHI_CSV, index=False)
    print(f"  Saved {len(kalshi_df)} rows to {KALSHI_CSV}")

    print("Refreshing Underdog Fantasy NBA player props...")
    data = fetch_underdog_data()
    ud_df = parse_nba_props(data)
    ud_df.to_csv(UNDERDOG_CSV, index=False)
    print(f"  Saved {len(ud_df)} rows to {UNDERDOG_CSV}")


def main():
    parser = argparse.ArgumentParser(
        description="Find +EV Underdog Fantasy legs using Kalshi implied probabilities.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # 2-leg entry at 3x base payout
  uv run ud-picks

  # 2-leg entry at 3x, only show picks with 5pp+ edge
  uv run ud-picks --min-edge 5

  # Refresh data first, then scan
  uv run ud-picks --refresh

  # 3-leg entry at 6x base payout
  uv run ud-picks --legs 3 --payout 6.0
        """,
    )
    parser.add_argument("--legs", type=int, default=2,
                        help="Number of legs in the entry (default: 2)")
    parser.add_argument("--payout", type=float, default=3.0,
                        help="Base payout multiplier for a standard entry at this leg count (default: 3.0)")
    parser.add_argument("--min-edge", type=float, default=0.0,
                        help="Minimum edge in percentage points to show (default: 0)")
    parser.add_argument("--top", type=int, default=20,
                        help="Show top N picks (default: 20)")
    parser.add_argument("--refresh", action="store_true",
                        help="Re-fetch Kalshi and Underdog data before scanning")
    parser.add_argument("--save", action="store_true",
                        help=f"Save all results to {UNDERDOG_PICKS_CSV}")
    args = parser.parse_args()

    if args.refresh:
        refresh_data()
        print()

    base_be = base_breakeven(args.legs, args.payout)
    print(f"{args.legs}-leg entry @ {args.payout}x base payout")
    print(f"Base break-even (1.0x leg): {base_be:.1f}%")
    print(f"  e.g. 0.75x leg needs {base_be/0.75:.1f}%  |  1.1x leg needs {base_be/1.1:.1f}%")
    print(f"Scanning for picks with edge >= {args.min_edge}pp...\n")

    picks = find_picks(legs=args.legs, payout=args.payout, min_edge=args.min_edge)

    if picks.empty:
        print("No picks found above their required probability.")
        return

    top = picks.head(args.top)
    print(f"Found {len(picks)} pick(s) | showing top {len(top)}:\n")
    print(top.to_string(index=False))
    print("\nColumns: ud_mult = Underdog payout multiplier for this leg")
    print("         required_prob = break-even accounting for ud_mult")
    print("         edge = kalshi_prob - required_prob (positive = +EV pick)")

    if args.save:
        picks.to_csv(UNDERDOG_PICKS_CSV, index=False)
        print(f"\nSaved {len(picks)} picks to {UNDERDOG_PICKS_CSV}")


if __name__ == "__main__":
    main()
