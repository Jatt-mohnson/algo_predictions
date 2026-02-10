import argparse
import os
import re
import sys
from datetime import datetime, timezone

import pandas as pd
from pykalshi import Action, OrderType, Side

from main import get_client, fetch_nba_player_props
from underdog import fetch_underdog_data, parse_nba_props

# Map Kalshi series tickers to Underdog stat names
SERIES_TO_STAT = {
    "KXNBAPTS": "Points",
    "KXNBAREB": "Rebounds",
    "KXNBAAST": "Assists",
    "KXNBA3PT": "3-Pointers Made",
    "KXNBASTL": "Steals",
    "KXNBABLK": "Blocks",
}

TRADES_LOG = "trades_log.csv"


def load_traded_keys():
    """Load the set of (ticker, side) pairs already traded."""
    if not os.path.exists(TRADES_LOG):
        return set()
    df = pd.read_csv(TRADES_LOG)
    return set(zip(df["ticker"], df["side"]))


def log_trade(ticker, action, side, count, order_type, price):
    """Append a placed trade to the log."""
    row = pd.DataFrame([{
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "ticker": ticker,
        "action": action.value,
        "side": side.value,
        "count": count,
        "order_type": order_type.value,
        "price": price,
    }])
    header = not os.path.exists(TRADES_LOG)
    row.to_csv(TRADES_LOG, mode="a", header=header, index=False)


def place_trade(client, ticker, action, side, count, order_type, price, dry_run=False):
    """Place a single trade with guardrails."""
    price_kwargs = {"yes_price": price} if side == Side.YES else {"no_price": price}

    cost = price * count if action == Action.BUY else (100 - price) * count
    print(f"  Ticker:  {ticker}")
    print(f"  Action:  {action.value}")
    print(f"  Side:    {side.value}")
    print(f"  Count:   {count}")
    print(f"  Type:    {order_type.value}")
    print(f"  Price:   {price}¢")
    print(f"  Est cost: {cost}¢ (${cost / 100:.2f})")

    if dry_run:
        print("  [DRY RUN] Order not placed.")
        return None

    order = client.portfolio.place_order(
        ticker=ticker,
        action=action,
        side=side,
        count=count,
        order_type=order_type,
        **price_kwargs,
    )
    print(f"  Order placed: {order}")
    return order


def parse_kalshi_title(title):
    """Extract player name and threshold from a Kalshi market title.

    Example: 'Victor Wembanyama: 35+ points' -> ('Victor Wembanyama', 35)
    """
    match = re.match(r"^(.+?):\s*(\d+)\+\s+\w+", title)
    if not match:
        return None, None
    return match.group(1).strip(), int(match.group(2))


def find_edges(kalshi_csv, underdog_csv, min_edge):
    """Find edges between Kalshi and Underdog markets.

    Returns a DataFrame of opportunities where the edge exceeds min_edge (in cents).
    """
    kalshi_df = pd.read_csv(kalshi_csv)
    underdog_df = pd.read_csv(underdog_csv)

    edges = []

    for _, row in kalshi_df.iterrows():
        player_name, threshold = parse_kalshi_title(row["title"])
        if player_name is None:
            continue

        # series_ticker may be empty; extract from ticker prefix instead
        series = row.get("series_ticker", "")
        if not series or (isinstance(series, float) and pd.isna(series)):
            # Ticker format: KXNBAPTS-26FEB10SASLAL-...
            series = row["ticker"].split("-")[0]
        stat_name = SERIES_TO_STAT.get(series)
        if not stat_name:
            continue

        yes_ask = row.get("yes_ask")
        no_ask = row.get("no_ask")
        if pd.isna(yes_ask) or pd.isna(no_ask) or yes_ask == 0 or no_ask == 0:
            continue

        # Kalshi "N+" means >= N, equivalent to Underdog "over N-0.5"
        underdog_threshold = threshold - 0.5

        # Find matching Underdog lines
        matches = underdog_df[
            (underdog_df["full_name"].str.lower() == player_name.lower())
            & (underdog_df["stat_name"].str.lower() == stat_name.lower())
            & (underdog_df["stat_value"] == underdog_threshold)
        ]

        for _, ud_row in matches.iterrows():
            payout = ud_row["payout_multiplier"]
            if pd.isna(payout) or payout <= 0:
                continue

            # Implied probability from Underdog: bet $1, profit = payout_multiplier
            ud_implied_prob = 1.0 / (1.0 + payout) * 100  # in cents scale

            choice = ud_row["choice"]
            base = {
                "ticker": row["ticker"],
                "player": player_name,
                "stat": stat_name,
                "threshold": threshold,
                "kalshi_yes_ask": yes_ask,
                "kalshi_no_ask": no_ask,
                "underdog_choice": choice,
                "underdog_payout_multiplier": payout,
                "underdog_implied": round(ud_implied_prob, 1),
            }

            if choice == "over":
                # Underdog says "over" is worth ud_implied_prob, Kalshi yes_ask is the cost
                edge = ud_implied_prob - yes_ask
                if edge >= min_edge:
                    edges.append({
                        **base,
                        "side": "yes",
                        "kalshi_price": yes_ask,
                        "edge": round(edge, 1),
                    })
            elif choice == "under":
                # Underdog says "under" is worth ud_implied_prob, Kalshi no_ask is the cost
                edge = ud_implied_prob - no_ask
                if edge >= min_edge:
                    edges.append({
                        **base,
                        "side": "no",
                        "kalshi_price": no_ask,
                        "edge": round(edge, 1),
                    })

    edges_df = pd.DataFrame(edges)
    if not edges_df.empty:
        edges_df = edges_df.sort_values("edge", ascending=False).reset_index(drop=True)
    return edges_df


def execute_edge_trades(client, edges_df, count, order_type, max_contracts, max_spend, dry_run, skip_confirm):
    """Place trades for each detected edge, respecting guardrails."""
    traded_keys = load_traded_keys()
    total_spend = 0
    skipped = 0

    for _, edge in edges_df.iterrows():
        side = Side.YES if edge["side"] == "yes" else Side.NO
        key = (edge["ticker"], edge["side"])

        if key in traded_keys:
            skipped += 1
            continue

        price = int(edge["kalshi_price"])
        order_count = min(count, max_contracts)
        cost = price * order_count

        if total_spend + cost > max_spend:
            remaining = max_spend - total_spend
            order_count = remaining // price
            if order_count <= 0:
                print(f"\nMax spend reached ({max_spend}¢). Stopping.")
                break

        print(f"\n--- Edge: {edge['edge']}¢ | {edge['player']} {edge['stat']} {edge['threshold']}+ ---")

        if not skip_confirm and not dry_run:
            response = input("Place this order? [y/N] ")
            if response.lower() != "y":
                print("  Skipped.")
                continue

        order = place_trade(client, edge["ticker"], Action.BUY, side, order_count, order_type, price, dry_run)
        total_spend += price * order_count

        if order is not None:
            log_trade(edge["ticker"], Action.BUY, side, order_count, order_type, price)
            traded_keys.add(key)

    if skipped:
        print(f"\nSkipped {skipped} edge(s) already traded (see {TRADES_LOG})")
    print(f"Total spend this run: {total_spend}¢ (${total_spend / 100:.2f})")


def cmd_manual(args):
    """Handle the manual subcommand."""
    action = Action(args.action)
    side = Side(args.side)
    order_type = OrderType(args.type)

    if args.count > args.max_contracts:
        print(f"Error: count ({args.count}) exceeds max-contracts ({args.max_contracts})")
        sys.exit(1)

    cost = args.price * args.count if action == Action.BUY else (100 - args.price) * args.count
    if cost > args.max_spend:
        print(f"Error: estimated cost ({cost}¢) exceeds max-spend ({args.max_spend}¢)")
        sys.exit(1)

    print("Order details:")
    if not args.yes and not args.dry_run:
        response = input("Place this order? [y/N] ")
        if response.lower() != "y":
            print("Cancelled.")
            return

    client = None if args.dry_run else get_client()
    place_trade(client, args.ticker, action, side, args.count, order_type, args.price, args.dry_run)


def refresh_data():
    """Re-fetch Kalshi and Underdog data, overwriting the CSVs."""
    client = get_client()

    print("Refreshing Kalshi markets...")
    kalshi_df = fetch_nba_player_props(client)
    kalshi_df.to_csv("nba_player_props.csv", index=False)
    print(f"  {len(kalshi_df)} markets saved to nba_player_props.csv")

    print("Refreshing Underdog lines...")
    ud_data = fetch_underdog_data()
    ud_df = parse_nba_props(ud_data)
    ud_df.to_csv("underdog_nba_props.csv", index=False)
    print(f"  {len(ud_df)} lines saved to underdog_nba_props.csv")


def cmd_auto(args):
    """Handle the auto subcommand."""
    order_type = OrderType(args.type)

    if args.refresh:
        refresh_data()
        print()

    print("Scanning for edges...")
    edges_df = find_edges("nba_player_props.csv", "underdog_nba_props.csv", args.min_edge)

    if edges_df.empty:
        print("No edges found.")
        return

    print(f"Found {len(edges_df)} edge(s):\n")
    print(edges_df.to_string(index=False))

    edges_df.to_csv("edges.csv", index=False)
    print(f"\nRaw edge data saved to edges.csv")
    print()

    client = None if args.dry_run else get_client()
    execute_edge_trades(client, edges_df, args.count, order_type, args.max_contracts, args.max_spend, args.dry_run, args.yes)


def main():
    parser = argparse.ArgumentParser(description="Kalshi trading script")
    parser.add_argument("--max-contracts", type=int, default=20, help="Max contracts per order (default: 20)")
    parser.add_argument("--max-spend", type=int, default=5000, help="Max total spend in cents (default: 5000 = $50)")
    parser.add_argument("--dry-run", action="store_true", help="Print trades without executing")
    parser.add_argument("--yes", "-y", action="store_true", help="Skip confirmation prompts")

    subparsers = parser.add_subparsers(dest="command", required=True)

    # Manual trade
    manual_parser = subparsers.add_parser("manual", help="Place a single trade manually")
    manual_parser.add_argument("--ticker", required=True, help="Market ticker")
    manual_parser.add_argument("--action", required=True, choices=["buy", "sell"], help="Buy or sell")
    manual_parser.add_argument("--side", required=True, choices=["yes", "no"], help="Yes or no side")
    manual_parser.add_argument("--count", required=True, type=int, help="Number of contracts")
    manual_parser.add_argument("--price", required=True, type=int, help="Price in cents (1-99)")
    manual_parser.add_argument("--type", default="limit", choices=["limit", "market"], help="Order type (default: limit)")

    # Auto edge detection
    auto_parser = subparsers.add_parser("auto", help="Find and trade edges vs Underdog")
    auto_parser.add_argument("--refresh", action="store_true", help="Re-fetch Kalshi and Underdog data before scanning")
    auto_parser.add_argument("--min-edge", type=int, default=5, help="Minimum edge in cents (default: 5)")
    auto_parser.add_argument("--count", type=int, default=5, help="Contracts per trade (default: 5)")
    auto_parser.add_argument("--type", default="limit", choices=["limit", "market"], help="Order type (default: limit)")

    args = parser.parse_args()

    if args.command == "manual":
        cmd_manual(args)
    elif args.command == "auto":
        cmd_auto(args)


if __name__ == "__main__":
    main()
