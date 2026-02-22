import argparse
import math
import os
import re
import sys
from datetime import datetime, timedelta, timezone

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
    CBB_KALSHI_CSV,
    CBB_DRAFTKINGS_CSV,
    CBB_PINNACLE_CSV,
    CBB_EDGES_CSV,
    parse_kalshi_title,
    parse_cbb_ticker,
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

    print("Refreshing Kalshi markets...")
    kalshi_df = fetch_nba_player_props(client)
    kalshi_df.to_csv(KALSHI_CSV, index=False)
    print(f"  {len(kalshi_df)} markets saved to {KALSHI_CSV}")


def _load_cbb_book_df(source):
    """Load CBB sportsbook CSV(s) based on source."""
    dfs = []
    if source in ("draftkings", "both"):
        dfs.append(("dk", pd.read_csv(CBB_DRAFTKINGS_CSV)))
    if source in ("pinnacle", "both"):
        dfs.append(("pinn", pd.read_csv(CBB_PINNACLE_CSV)))
    return dfs


def _normalize_cbb_name(name):
    """Normalize a CBB team/game name for fuzzy matching.

    Handles differences like: @ vs at, St. vs State, SE vs Southeastern,
    abbreviated school names, etc.
    """
    s = name.lower().strip()
    # Normalize separators
    s = s.replace(" @ ", " at ")
    s = s.replace(" vs ", " at ").replace(" vs. ", " at ")
    # Normalize common abbreviations
    s = re.sub(r"\bst\.\b", "state", s)
    s = re.sub(r"\bse\b", "southeastern", s)
    s = re.sub(r"\bmd\b", "maryland", s)
    s = re.sub(r"\bnc\b", "north carolina", s)
    # Strip punctuation (hyphens, periods, apostrophes)
    s = re.sub(r"[.\-']", " ", s)
    # Collapse whitespace
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _cbb_name_tokens(name):
    """Extract a set of meaningful tokens from a normalized CBB name."""
    normalized = _normalize_cbb_name(name)
    # Remove common filler words
    stop = {"at", "the", "of", "and"}
    return {t for t in normalized.split() if t not in stop}


def _match_cbb_game(kalshi_title, book_df):
    """Try to match a Kalshi CBB game to a sportsbook game by title.

    Uses token overlap to handle name differences between platforms.
    Returns matching rows from book_df, or empty DataFrame.
    """
    # Extract game portion: "Team1 at Team2: N+ total points" -> "Team1 at Team2"
    game_part = kalshi_title.split(":")[0].strip() if ":" in kalshi_title else kalshi_title
    kalshi_tokens = _cbb_name_tokens(game_part)

    if not kalshi_tokens:
        return pd.DataFrame()

    best_score = 0
    best_idx = None
    for idx, row in book_df.iterrows():
        book_tokens = _cbb_name_tokens(row["game"])
        if not book_tokens:
            continue
        overlap = len(kalshi_tokens & book_tokens)
        # Require decent overlap relative to the smaller token set
        score = overlap / min(len(kalshi_tokens), len(book_tokens))
        if score > best_score:
            best_score = score
            best_idx = idx

    # Require at least 60% token overlap, then return ALL rows for that game
    if best_score >= 0.6 and best_idx is not None:
        matched_game = book_df.loc[best_idx, "game"]
        return book_df[book_df["game"] == matched_game]
    return pd.DataFrame()


def find_cbb_edges(kalshi_csv, min_price, max_price, min_edge, source="both", max_spread=5, max_line_diff=2):
    """Find CBB total edges based on thesis and optional sportsbook validation.

    Returns a DataFrame of opportunities.
    """
    if not os.path.exists(kalshi_csv):
        print(f"No Kalshi CBB data found at {kalshi_csv}")
        print("Run 'uv run cbb-kalshi' first, or use --refresh to fetch inline.")
        return pd.DataFrame()

    kalshi_df = pd.read_csv(kalshi_csv)

    # Load sportsbook data if available
    book_dfs = []
    if source != "none":
        try:
            book_dfs = _load_cbb_book_df(source)
        except FileNotFoundError:
            pass

    edges = []
    for _, row in kalshi_df.iterrows():
        title = row.get("title", "")
        ticker = row["ticker"]
        yes_ask = row.get("yes_ask")
        yes_bid = row.get("yes_bid")
        no_ask = row.get("no_ask")

        if pd.isna(yes_ask) or yes_ask == 0:
            continue

        # Skip games that are likely live: expected_expiration_time is within 2.5 hours from now
        exp_time_str = row.get("expected_expiration_time")
        if exp_time_str and not pd.isna(exp_time_str):
            try:
                exp_time = datetime.fromisoformat(str(exp_time_str).replace("Z", "+00:00"))
                if datetime.now(timezone.utc) > exp_time - timedelta(hours=2.5):
                    continue
            except (ValueError, TypeError):
                pass

        spread = round(yes_ask - yes_bid, 1) if not pd.isna(yes_bid) and yes_bid > 0 else None
        if spread is not None and spread > max_spread:
            continue

        # Parse threshold from ticker
        _, _, threshold = parse_cbb_ticker(ticker)
        if threshold is None:
            continue

        # Filter to overs in the thesis price range
        if not (min_price <= yes_ask <= max_price):
            continue

        thesis_edge = 40 - yes_ask

        # Sportsbook validation
        book_implied_over = None
        book_edge = None
        book_line_val = None
        dk_implied = ""
        pinn_implied = ""

        if book_dfs:
            implieds = []
            # Kalshi "N+" means >= N, which maps to sportsbook "over N-0.5"
            target_line = threshold - 0.5
            for label, bdf in book_dfs:
                matches = _match_cbb_game(title, bdf)
                if matches.empty:
                    continue

                available_lines = sorted(matches["total_line"].dropna().unique())
                if len(available_lines) == 0:
                    continue

                # Prefer exact line match, then closest available line
                if target_line in available_lines:
                    book_line = target_line
                else:
                    book_line = min(available_lines, key=lambda x: abs(x - target_line))

                # Skip if the closest book line is too far from the Kalshi threshold
                if abs(book_line - target_line) > max_line_diff:
                    continue

                all_sides = matches[matches["total_line"] == book_line]
                implied = _compute_vig_free_implied(all_sides)
                if implied and "over" in implied:
                    imp_val = implied["over"]
                    implieds.append((imp_val, book_line))
                    if label == "dk":
                        dk_implied = round(imp_val, 1)
                    elif label == "pinn":
                        pinn_implied = round(imp_val, 1)

            if implieds:
                avg_implied = sum(iv for iv, _ in implieds) / len(implieds)
                avg_book_line = sum(bl for _, bl in implieds) / len(implieds)
                book_implied_over = avg_implied
                book_edge = round(book_implied_over - yes_ask, 1)
                book_line_val = avg_book_line

        # When source is "none", use thesis edge (target: 40¢ fair value)
        if book_edge is None:
            if source != "none":
                continue
            book_edge = round(thesis_edge, 1)

        edge_row = {
            "ticker": ticker,
            "game": title.split(":")[0].strip() if ":" in title else title,
            "threshold": threshold,
            "kalshi_bid": int(yes_bid) if yes_bid and not pd.isna(yes_bid) else "",
            "kalshi_ask": int(yes_ask),
            "spread": spread if spread is not None else "",
            "book_line": book_line_val,
            "dk_implied": dk_implied,
            "pinn_implied": pinn_implied,
            "book_implied": round(book_implied_over, 1) if book_implied_over else "",
            "book_edge": book_edge,
            "side": "yes",
            "kalshi_price": int(yes_ask),
            "_tradeable": book_edge >= min_edge,
        }
        edges.append(edge_row)

    edges_df = pd.DataFrame(edges)
    if not edges_df.empty:
        edges_df = edges_df.sort_values("book_edge", ascending=False).reset_index(drop=True)
    return edges_df


def refresh_cbb_data(source="both"):
    """Re-fetch Kalshi CBB and sportsbook data."""
    from src.cbb_main import fetch_cbb_totals

    client = get_client()

    print("Refreshing Kalshi CBB totals...")
    df = fetch_cbb_totals(client)
    if not df.empty:
        parsed = df["ticker"].apply(
            lambda t: pd.Series(parse_cbb_ticker(t), index=["team1", "team2", "threshold_parsed"])
        )
        df = pd.concat([df, parsed], axis=1)
    df.to_csv(CBB_KALSHI_CSV, index=False)
    print(f"  {len(df)} markets saved to {CBB_KALSHI_CSV}")

    if source in ("draftkings", "both"):
        print("Refreshing DraftKings CBB totals...")
        try:
            from src.cbb_draftkings import fetch_game_totals, parse_totals
            data = fetch_game_totals()
            rows = parse_totals(data)
            dk_df = pd.DataFrame(rows)
            if len(dk_df) > 0:
                dk_df.to_csv(CBB_DRAFTKINGS_CSV, index=False)
            print(f"  {len(dk_df)} lines saved to {CBB_DRAFTKINGS_CSV}")
        except Exception as e:
            print(f"  DraftKings error: {e}")

    if source in ("pinnacle", "both"):
        print("Refreshing Pinnacle CBB totals...")
        try:
            from src.cbb_pinnacle import fetch_matchups, parse_game_totals, fetch_total_prices
            from src.pinnacle import american_to_decimal
            matchups = fetch_matchups()
            games = parse_game_totals(matchups)
            if games.empty:
                print("  No Pinnacle NCAAB games found.")
            else:
                matchup_ids = games["matchup_id"].unique().tolist()
                prices = fetch_total_prices(matchup_ids, include_alternates=True)
                if prices.empty:
                    print("  No Pinnacle total prices found.")
                else:
                    merged = games.merge(prices, on="matchup_id", how="inner")
                    merged["odds_decimal"] = merged["odds_american"].apply(american_to_decimal)
                    result = merged[["game", "total_line", "choice", "odds_decimal", "odds_american"]].copy()
                    result.to_csv(CBB_PINNACLE_CSV, index=False)
                    print(f"  {len(result)} lines saved to {CBB_PINNACLE_CSV}")
        except Exception as e:
            print(f"  Pinnacle error: {e}")


def cmd_auto_cbb(args):
    """Handle the auto-cbb subcommand."""
    order_type = OrderType(args.type)

    if args.refresh:
        refresh_cbb_data(source=args.source)
        print()

    print(f"Scanning CBB totals for thesis edges (overs priced {args.min_price}-{args.max_price}¢, max spread {args.max_spread}¢)...")
    edges_df = find_cbb_edges(
        CBB_KALSHI_CSV, args.min_price, args.max_price, args.min_edge,
        source=args.source, max_spread=args.max_spread, max_line_diff=args.max_line_diff,
    )

    if edges_df.empty:
        print("No opportunities found.")
        return

    # Display columns (exclude internal _tradeable flag)
    display_cols = [c for c in edges_df.columns if not c.startswith("_")]
    print(f"Found {len(edges_df)} opportunity(ies):\n")
    print(edges_df[display_cols].to_string(index=False))

    # Save raw edges
    edges_df[display_cols].to_csv(CBB_EDGES_CSV, index=False)
    print(f"\nEdge data saved to {CBB_EDGES_CSV}")

    # Filter to tradeable edges only
    tradeable = edges_df[edges_df["_tradeable"] == True]  # noqa: E712
    if tradeable.empty:
        print("\nNo edges meet minimum edge threshold for trading.")
        return

    print(f"\n{len(tradeable)} edge(s) meet trading criteria.")
    print()

    # Map CBB columns to what execute_edge_trades expects
    tradeable = tradeable.copy()
    tradeable["edge"] = tradeable["book_edge"]
    tradeable["player"] = tradeable["game"]
    tradeable["stat"] = "Total"

    client = None if args.dry_run else get_client()
    execute_edge_trades(
        client, tradeable, args.count, order_type,
        args.max_contracts, args.max_spend, args.dry_run, args.yes
    )


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

    # Auto CBB totals edge detection
    cbb_parser = subparsers.add_parser("auto-cbb", help="Find and trade CBB total edges (thesis + sportsbook)")
    cbb_parser.add_argument("--source", default="both", choices=["draftkings", "pinnacle", "both", "none"],
                            help="Odds source for edge validation (default: both, 'none' for thesis-only)")
    cbb_parser.add_argument("--refresh", action="store_true", help="Re-fetch Kalshi and sportsbook data before scanning")
    cbb_parser.add_argument("--min-price", type=int, default=20, help="Minimum Kalshi ask price in cents (default: 20)")
    cbb_parser.add_argument("--max-price", type=int, default=30, help="Maximum Kalshi ask price in cents (default: 30)")
    cbb_parser.add_argument("--min-edge", type=int, default=0, help="Minimum sportsbook edge in cents to trade (default: 0, thesis-only)")
    cbb_parser.add_argument("--max-spread", type=int, default=5, help="Max bid-ask spread in cents to include (default: 5)")
    cbb_parser.add_argument("--max-line-diff", type=float, default=2, help="Max allowed difference between Kalshi threshold and book line (default: 2)")
    cbb_parser.add_argument("--count", type=int, default=5, help="Contracts per trade (default: 5)")
    cbb_parser.add_argument("--type", default="limit", choices=["limit", "market"], help="Order type (default: limit)")

    args = parser.parse_args()

    if args.command == "manual":
        cmd_manual(args)
    elif args.command == "auto":
        cmd_auto(args)
    elif args.command == "auto-cbb":
        cmd_auto_cbb(args)


if __name__ == "__main__":
    main()
