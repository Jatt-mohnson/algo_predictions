"""Join all data sources into a single comparison view.

Reads CSV files from Kalshi, Underdog, DraftKings, and Pinnacle and produces
a unified table showing each player/stat/threshold with odds from every source.

Output: combined_odds.csv
"""

import os
import sys

import pandas as pd

from src.common import (
    SERIES_TO_STAT,
    KALSHI_CSV,
    DRAFTKINGS_CSV,
    PINNACLE_CSV,
    UNDERDOG_CSV,
    COMBINED_CSV,
    parse_kalshi_title,
)


def load_kalshi(path=KALSHI_CSV):
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
        rows.append({
            "player": player,
            "stat": stat,
            "threshold": threshold,
            "ticker": row["ticker"],
            "kalshi_yes_bid": row.get("yes_bid"),
            "kalshi_yes_ask": row.get("yes_ask"),
            "kalshi_no_bid": row.get("no_bid"),
            "kalshi_no_ask": row.get("no_ask"),
        })
    return pd.DataFrame(rows)


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
    kalshi = load_kalshi()
    if kalshi.empty:
        print("No Kalshi data found. Run main.py first.")
        sys.exit(1)

    # Sportsbook thresholds are N-0.5 where Kalshi uses N+
    # Create a join key on sportsbook side: threshold + 0.5 maps to Kalshi threshold
    dk = load_sportsbook(DRAFTKINGS_CSV, "dk")
    pinn = load_sportsbook(PINNACLE_CSV, "pinn")
    ud = load_sportsbook(UNDERDOG_CSV, "ud")

    for book_df in [dk, pinn, ud]:
        if not book_df.empty:
            book_df["threshold"] = book_df["threshold"] + 0.5

    # Start with Kalshi as the base
    combined = kalshi.copy()

    # Merge each sportsbook
    join_cols = ["player", "stat", "threshold"]
    for book_df in [dk, pinn, ud]:
        if book_df.empty:
            continue
        # Normalize player names for matching
        book_df["_join_player"] = book_df["player"].str.lower().str.strip()
        combined["_join_player"] = combined["player"].str.lower().str.strip()
        combined["_join_stat"] = combined["stat"].str.lower().str.strip()
        book_df["_join_stat"] = book_df["stat"].str.lower().str.strip()

        combined = combined.merge(
            book_df.drop(columns=["player", "stat"]),
            left_on=["_join_player", "_join_stat", "threshold"],
            right_on=["_join_player", "_join_stat", "threshold"],
            how="left",
        )
        combined = combined.drop(columns=["_join_player", "_join_stat"], errors="ignore")

    # Order columns nicely
    desired_order = [
        "player", "stat", "threshold", "ticker",
        "kalshi_yes_bid", "kalshi_yes_ask", "kalshi_no_bid", "kalshi_no_ask",
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
    for col in ["dk_over", "pinn_over", "ud_over"]:
        if col in combined.columns:
            matched = combined[col].notna().sum()
            label = col.replace("_over", "").upper()
            print(f"  {label} matched: {matched}/{total}")


if __name__ == "__main__":
    main()
