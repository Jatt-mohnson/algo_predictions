import argparse
import math
import os
import sys
from datetime import datetime, timezone

import pandas as pd
from pykalshi import Action, OrderType, Side

from src.common import (
    SERIES_TO_STAT,
    KALSHI_STATS,
    KALSHI_CSV,
    DRAFTKINGS_CSV,
    PINNACLE_CSV,
    EDGES_CSV,
    TRADES_LOG_CSV,
    parse_kalshi_title,
)
from src.main import get_client, fetch_nba_player_props
from src.draftkings import fetch_subcategory, parse_props, SUBCATEGORY_MAP
from src.pinnacle import (
    fetch_matchups as pinn_fetch_matchups,
    parse_props as pinn_parse_props,
    fetch_all_prices as pinn_fetch_prices,
    american_to_decimal as pinn_american_to_decimal,
    CATEGORY_MAP as PINN_CATEGORY_MAP,
)

# Kalshi fee coefficients (per their fee schedule)
TAKER_FEE_COEFF = 0.07
MAKER_FEE_COEFF = 0.0175


def estimate_fee(price_cents, count, coeff=TAKER_FEE_COEFF):
    """Estimate Kalshi fee in cents.

    Formula: ceil(coeff × count × P × (1-P)) where P = price_cents / 100.
    """
    p = price_cents / 100.0
    return math.ceil(coeff * count * p * (1 - p))


def load_traded_keys():
    """Load the set of (ticker, side) pairs already traded."""
    if not os.path.exists(TRADES_LOG_CSV):
        return set()
    df = pd.read_csv(TRADES_LOG_CSV)
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
    header = not os.path.exists(TRADES_LOG_CSV)
    row.to_csv(TRADES_LOG_CSV, mode="a", header=header, index=False)


def place_trade(client, ticker, action, side, count, order_type, price, dry_run=False):
    """Place a single trade with guardrails."""
    price_kwargs = {"yes_price": price} if side == Side.YES else {"no_price": price}

    cost = price * count if action == Action.BUY else (100 - price) * count
    fee = estimate_fee(price, count)
    print(f"  Ticker:  {ticker}")
    print(f"  Action:  {action.value}")
    print(f"  Side:    {side.value}")
    print(f"  Count:   {count}")
    print(f"  Type:    {order_type.value}")
    print(f"  Price:   {price}¢")
    print(f"  Est cost: {cost}¢ (${cost / 100:.2f})")
    fee_pct = (fee / cost * 100) if cost > 0 else 0
    print(f"  Est fee:  {fee}¢ (${fee / 100:.2f}) [{fee_pct:.1f}% of cost, taker]")

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


def _compute_vig_free_implied(matches):
    """Compute vig-free implied probabilities for over/under rows.

    Returns a dict mapping choice -> vig-free implied probability (in cents scale),
    or None if we can't compute (missing over or under).
    """
    over_row = matches[matches["choice"] == "over"]
    under_row = matches[matches["choice"] == "under"]
    if over_row.empty or under_row.empty:
        return None
    over_odds = over_row.iloc[0]["odds_decimal"]
    under_odds = under_row.iloc[0]["odds_decimal"]
    if pd.isna(over_odds) or pd.isna(under_odds) or over_odds <= 0 or under_odds <= 0:
        return None
    raw_over = 1.0 / over_odds
    raw_under = 1.0 / under_odds
    overround = raw_over + raw_under
    result = {}
    for _, r in matches.iterrows():
        odds = r["odds_decimal"]
        if pd.isna(odds) or odds <= 0:
            continue
        result[r["choice"]] = (1.0 / odds / overround) * 100
    return result


def _load_book_df(source):
    """Load sportsbook CSV(s) based on source. Returns list of (label, DataFrame) pairs."""
    dfs = []
    if source in ("draftkings", "both"):
        dfs.append(("dk", pd.read_csv(DRAFTKINGS_CSV)))
    if source in ("pinnacle", "both"):
        dfs.append(("pinn", pd.read_csv(PINNACLE_CSV)))
    return dfs


def _parse_kalshi_market(row):
    """Extract player, stat, threshold, and prices from a Kalshi market row.

    Returns a dict with the parsed fields, or None if the row is invalid.
    """
    player_name, threshold = parse_kalshi_title(row["title"])
    if player_name is None:
        return None

    series = row.get("series_ticker", "")
    if not series or (isinstance(series, float) and pd.isna(series)):
        series = row["ticker"].split("-")[0]
    stat_name = SERIES_TO_STAT.get(series)
    if not stat_name:
        return None

    yes_ask = row.get("yes_ask")
    no_ask = row.get("no_ask")
    if pd.isna(yes_ask) or pd.isna(no_ask) or yes_ask == 0 or no_ask == 0:
        return None

    return {
        "ticker": row["ticker"],
        "player": player_name,
        "stat": stat_name,
        "threshold": threshold,
        "yes_ask": yes_ask,
        "no_ask": no_ask,
    }


def _lookup_book_implied(market, book_dfs):
    """Look up vig-free implied probabilities across sportsbooks for a market.

    Returns a dict: label -> {choice -> implied_prob}, e.g. {"dk": {"over": 55.2, "under": 44.8}}.
    """
    book_threshold = market["threshold"] - 0.5
    player_lower = market["player"].lower()
    stat_lower = market["stat"].lower()

    book_implied = {}
    for label, df in book_dfs:
        matches = df[
            (df["full_name"].str.lower() == player_lower)
            & (df["stat_name"].str.lower() == stat_lower)
            & (df["stat_value"] == book_threshold)
        ]
        implied = _compute_vig_free_implied(matches)
        if implied:
            book_implied[label] = implied
    return book_implied


def _compute_edge(market, book_implied, source):
    """Compute edges for a single market given sportsbook implied probabilities.

    Returns a list of edge dicts (one per side that exceeds 0), or empty list.
    """
    edges = []
    for choice in ("over", "under"):
        implieds = {label: imp[choice] for label, imp in book_implied.items() if choice in imp}
        if not implieds:
            continue

        avg_implied = sum(implieds.values()) / len(implieds)
        kalshi_price = market["yes_ask"] if choice == "over" else market["no_ask"]
        edge_val = avg_implied - kalshi_price

        base = {
            "ticker": market["ticker"],
            "player": market["player"],
            "stat": market["stat"],
            "threshold": market["threshold"],
            "kalshi_yes_ask": market["yes_ask"],
            "kalshi_no_ask": market["no_ask"],
            "choice": choice,
        }

        if source == "both":
            base["dk_implied"] = round(implieds["dk"], 1) if "dk" in implieds else ""
            base["pinn_implied"] = round(implieds["pinn"], 1) if "pinn" in implieds else ""
            base["avg_implied"] = round(avg_implied, 1)
        elif source == "draftkings":
            base["dk_implied"] = round(avg_implied, 1)
        elif source == "pinnacle":
            base["pinn_implied"] = round(avg_implied, 1)

        edges.append({
            **base,
            "side": "yes" if choice == "over" else "no",
            "kalshi_price": kalshi_price,
            "edge": round(edge_val, 1),
        })
    return edges


def find_edges(kalshi_csv, min_edge, source="both"):
    """Find edges between Kalshi and sportsbook markets.

    source: "draftkings", "pinnacle", or "both" (averages implied probabilities).
    Returns a DataFrame of opportunities where the edge exceeds min_edge (in cents).
    """
    kalshi_df = pd.read_csv(kalshi_csv)
    book_dfs = _load_book_df(source)

    edges = []
    for _, row in kalshi_df.iterrows():
        market = _parse_kalshi_market(row)
        if not market:
            continue

        book_implied = _lookup_book_implied(market, book_dfs)
        if not book_implied:
            continue

        for edge in _compute_edge(market, book_implied, source):
            if edge["edge"] >= min_edge:
                edges.append(edge)

    edges_df = pd.DataFrame(edges)
    if not edges_df.empty:
        edges_df = edges_df.sort_values("edge", ascending=False).reset_index(drop=True)
    return edges_df


def execute_edge_trades(client, edges_df, count, order_type, max_contracts, max_spend, dry_run, skip_confirm):
    """Place trades for each detected edge, respecting guardrails."""
    traded_keys = load_traded_keys()
    total_spend = 0
    total_fees = 0
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
        total_fees += estimate_fee(price, order_count)

        if order is not None:
            log_trade(edge["ticker"], Action.BUY, side, order_count, order_type, price)
            traded_keys.add(key)

    if skipped:
        print(f"\nSkipped {skipped} edge(s) already traded (see {TRADES_LOG_CSV})")
    print(f"Total spend this run: {total_spend}¢ (${total_spend / 100:.2f})")
    fees_pct = (total_fees / total_spend * 100) if total_spend > 0 else 0
    print(f"Total est fees:       {total_fees}¢ (${total_fees / 100:.2f}) [{fees_pct:.1f}% of spend, taker]")


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


def refresh_data(source="both"):
    """Re-fetch Kalshi and sportsbook data, overwriting the CSVs."""
    client = get_client()

    print("Refreshing Kalshi markets...")
    kalshi_df = fetch_nba_player_props(client)
    kalshi_df.to_csv(KALSHI_CSV, index=False)
    print(f"  {len(kalshi_df)} markets saved to {KALSHI_CSV}")
    
    if source in ("pinnacle", "both"):
        print("Refreshing Pinnacle lines...")
        matchups = pinn_fetch_matchups()
        props = pinn_parse_props(matchups)
        if props.empty:
            print("  No Pinnacle props found.")
        else:
            props["stat_name"] = props["category"].map(PINN_CATEGORY_MAP)
            props = props.dropna(subset=["stat_name"])
            props = props[props["stat_name"].isin(KALSHI_STATS)]
            if props.empty:
                print("  No Kalshi-supported Pinnacle props found.")
            else:
                matchup_ids = props["matchup_id"].unique().tolist()
                prices = pinn_fetch_prices(matchup_ids)
                if prices.empty:
                    print("  No Pinnacle prices found.")
                else:
                    merged = props.merge(prices, on="participant_id", how="inner")
                    merged["odds_decimal"] = merged["odds_american"].apply(pinn_american_to_decimal)
                    result = merged[["player", "stat_name", "stat_value", "choice", "odds_decimal", "odds_american"]].copy()
                    result = result.rename(columns={"player": "full_name"})
                    result.to_csv(PINNACLE_CSV, index=False)
                    print(f"  {len(result)} lines saved to {PINNACLE_CSV}")

    if source in ("draftkings", "both"):
        print("Refreshing DraftKings lines...")
        all_rows = []
        for subcategory_id, stat_name in SUBCATEGORY_MAP.items():
            if stat_name not in KALSHI_STATS:
                continue
            try:
                data = fetch_subcategory(subcategory_id)
                all_rows.extend(parse_props(data, stat_name))
            except Exception as e:
                print(f"  {stat_name} (subcategory {subcategory_id}): error — {e}")
        dk_df = pd.DataFrame(all_rows)
        if len(dk_df) > 0:
            dk_df.to_csv(DRAFTKINGS_CSV, index=False)
        print(f"  {len(dk_df)} lines saved to {DRAFTKINGS_CSV}")


def cmd_auto(args):
    """Handle the auto subcommand."""
    order_type = OrderType(args.type)

    if args.refresh:
        refresh_data(source=args.source)
        print()

    print(f"Scanning for edges (source: {args.source})...")
    edges_df = find_edges(KALSHI_CSV, args.min_edge, source=args.source)

    if edges_df.empty:
        print("No edges found.")
        return

    print(f"Found {len(edges_df)} edge(s):\n")
    print(edges_df.to_string(index=False))

    edges_df.to_csv(EDGES_CSV, index=False)
    print(f"\nRaw edge data saved to {EDGES_CSV}")
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
    auto_parser = subparsers.add_parser("auto", help="Find and trade edges vs sportsbooks")
    auto_parser.add_argument("--source", default="both", choices=["draftkings", "pinnacle", "both"],
                             help="Odds source for edge detection (default: both)")
    auto_parser.add_argument("--refresh", action="store_true", help="Re-fetch Kalshi and sportsbook data before scanning")
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
