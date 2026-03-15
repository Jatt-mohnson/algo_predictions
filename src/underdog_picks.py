"""Find Underdog Fantasy picks using an external probability source (Kalshi or DraftKings).

Reads Underdog lines and compares them against implied probabilities from either
Kalshi markets or DraftKings decimal odds to surface +EV legs.

Each leg on Underdog has its own payout multiplier (over/under can differ).
Break-even per leg accounts for both the entry payout and the leg's multiplier:

  base_be       = (1 / total_payout) ^ (1 / legs)  × 100
  required_prob = base_be / ud_multiplier

A leg is +EV when: implied_prob > required_prob

Example — 1.5 3PM line, 2-leg entry at 3x base:
  over  (0.75x):  required = 57.7 / 0.75 = 76.9%
  under (1.1x):   required = 57.7 / 1.10 = 52.5%
"""

import argparse
import os
import sys

import pandas as pd

from src.common import (
    SERIES_TO_STAT,
    KALSHI_CSV,
    DRAFTKINGS_CSV,
    PINNACLE_CSV,
    UNDERDOG_CSV,
    DATA_DIR,
    parse_kalshi_title,
    normalize_player_name,
    adjust_prob_for_threshold,
)

UNDERDOG_PICKS_CSV = os.path.join(DATA_DIR, "underdog_picks.csv")


def base_breakeven(legs: int, payout: float) -> float:
    """Base break-even probability (0-100) assuming a 1.0x leg multiplier."""
    return ((1.0 / payout) ** (1.0 / legs)) * 100


def required_prob(base_be: float, ud_multiplier: float) -> float:
    """Adjusted break-even for a specific leg given its Underdog payout multiplier."""
    return base_be / ud_multiplier


def _is_standard_mult(val) -> bool:
    """True if a payout_multiplier is 1.0 (standard line) or missing."""
    return pd.isna(val) or abs(float(val) - 1.0) < 0.01


def load_kalshi_probs(path: str = KALSHI_CSV) -> pd.DataFrame:
    """Load Kalshi markets and return implied probabilities (bid/ask midpoint).

    Returns DataFrame with columns:
      _join_player, _join_stat, threshold, over_prob, under_prob, ticker
    """
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return pd.DataFrame()
    try:
        df = pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()
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
        over_prob = (yes_bid + yes_ask) / 2 if pd.notna(yes_bid) and pd.notna(yes_ask) else None
        under_prob = (no_bid + no_ask) / 2 if pd.notna(no_bid) and pd.notna(no_ask) else None
        if over_prob is None and under_prob is None:
            continue
        rows.append({
            "player": player,
            "stat": stat,
            "_join_player": normalize_player_name(player),
            "_join_stat": stat.lower().strip(),
            "threshold": threshold,
            "over_prob": round(over_prob, 1) if over_prob is not None else None,
            "under_prob": round(under_prob, 1) if under_prob is not None else None,
            "ticker": row["ticker"],
        })
    return pd.DataFrame(rows)


def load_dk_probs(path: str = DRAFTKINGS_CSV) -> pd.DataFrame:
    """Load DraftKings odds and return vig-free implied probabilities.

    Returns DataFrame with columns:
      _join_player, _join_stat, threshold, over_prob, under_prob
    """
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return pd.DataFrame()
    try:
        df = pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()
    if "odds_decimal" not in df.columns:
        return pd.DataFrame()

    over_df = df[df["choice"] == "over"][["full_name", "stat_name", "stat_value", "odds_decimal"]].copy()
    under_df = df[df["choice"] == "under"][["full_name", "stat_name", "stat_value", "odds_decimal"]].copy()
    over_df = over_df.rename(columns={"odds_decimal": "over_odds"})
    under_df = under_df.rename(columns={"odds_decimal": "under_odds"})

    merged = over_df.merge(under_df, on=["full_name", "stat_name", "stat_value"], how="inner")
    merged = merged[
        merged["over_odds"].notna() & merged["under_odds"].notna() &
        (merged["over_odds"] > 0) & (merged["under_odds"] > 0)
    ]

    # Remove vig: normalize so implied probs sum to 100%
    raw_over = 1.0 / merged["over_odds"]
    raw_under = 1.0 / merged["under_odds"]
    overround = raw_over + raw_under
    merged["over_prob"] = (raw_over / overround * 100).round(1)
    merged["under_prob"] = (raw_under / overround * 100).round(1)

    # DraftKings stat_value is N-0.5; add 0.5 to match Kalshi/Underdog threshold key
    merged["threshold"] = (merged["stat_value"] + 0.5).round(0).astype(int)
    merged["player"] = merged["full_name"]
    merged["stat"] = merged["stat_name"]
    merged["_join_player"] = merged["full_name"].apply(normalize_player_name)
    merged["_join_stat"] = merged["stat_name"].str.lower().str.strip()

    return merged[["player", "stat", "_join_player", "_join_stat", "threshold", "over_prob", "under_prob"]]


def load_pinnacle_probs(path: str = PINNACLE_CSV) -> pd.DataFrame:
    """Load Pinnacle odds and return vig-free implied probabilities.

    Returns DataFrame with columns:
      _join_player, _join_stat, threshold, over_prob, under_prob
    """
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return pd.DataFrame()
    try:
        df = pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()
    if "odds_decimal" not in df.columns:
        return pd.DataFrame()

    over_df = df[df["choice"] == "over"][["full_name", "stat_name", "stat_value", "odds_decimal"]].copy()
    under_df = df[df["choice"] == "under"][["full_name", "stat_name", "stat_value", "odds_decimal"]].copy()
    over_df = over_df.rename(columns={"odds_decimal": "over_odds"})
    under_df = under_df.rename(columns={"odds_decimal": "under_odds"})

    merged = over_df.merge(under_df, on=["full_name", "stat_name", "stat_value"], how="inner")
    merged = merged[
        merged["over_odds"].notna() & merged["under_odds"].notna() &
        (merged["over_odds"] > 0) & (merged["under_odds"] > 0)
    ]

    # Remove vig: normalize so implied probs sum to 100%
    raw_over = 1.0 / merged["over_odds"]
    raw_under = 1.0 / merged["under_odds"]
    overround = raw_over + raw_under
    merged["over_prob"] = (raw_over / overround * 100).round(1)
    merged["under_prob"] = (raw_under / overround * 100).round(1)

    # Pinnacle stat_value is N-0.5; add 0.5 to match Kalshi/Underdog threshold key
    merged["threshold"] = (merged["stat_value"] + 0.5).round(0).astype(int)
    merged["player"] = merged["full_name"]
    merged["stat"] = merged["stat_name"]
    merged["_join_player"] = merged["full_name"].apply(normalize_player_name)
    merged["_join_stat"] = merged["stat_name"].str.lower().str.strip()

    return merged[["player", "stat", "_join_player", "_join_stat", "threshold", "over_prob", "under_prob"]]


def load_probs(source: str) -> pd.DataFrame:
    """Load probability data from the specified source.

    For 'both', averages all available sources (Kalshi, DraftKings, Pinnacle),
    falling back to whichever subset is available.
    Returns DataFrame with _join_player, _join_stat, threshold, over_prob, under_prob,
    and optionally ticker (Kalshi only).
    """
    if source == "kalshi":
        return load_kalshi_probs()

    if source == "draftkings":
        return load_dk_probs()

    if source == "pinnacle":
        return load_pinnacle_probs()

    # both: merge and average all available sources
    all_sources = [
        ("_k", load_kalshi_probs()),
        ("_dk", load_dk_probs()),
        ("_p", load_pinnacle_probs()),
    ]
    available = [(sfx, df) for sfx, df in all_sources if not df.empty]

    if not available:
        return pd.DataFrame()
    if len(available) == 1:
        return available[0][1]

    # Rename prob/player/stat columns with source suffix before merging
    keys = ["_join_player", "_join_stat", "threshold"]
    has_ticker = False
    renamed = []
    for sfx, df in available:
        d = df.rename(columns={
            "over_prob": f"over_prob{sfx}",
            "under_prob": f"under_prob{sfx}",
            "player": f"player{sfx}",
            "stat": f"stat{sfx}",
        })
        if sfx == "_k" and "ticker" in d.columns:
            has_ticker = True
        renamed.append((sfx, d))

    result = renamed[0][1]
    for _, d in renamed[1:]:
        result = result.merge(d, on=keys, how="outer")

    suffixes = [sfx for sfx, _ in available]
    over_cols = [f"over_prob{sfx}" for sfx in suffixes if f"over_prob{sfx}" in result.columns]
    under_cols = [f"under_prob{sfx}" for sfx in suffixes if f"under_prob{sfx}" in result.columns]
    result["over_prob"] = result[over_cols].mean(axis=1, skipna=True).round(1)
    result["under_prob"] = result[under_cols].mean(axis=1, skipna=True).round(1)

    # Prefer Kalshi display names; fall back to DK then Pinnacle
    player_cols = [f"player{sfx}" for sfx in suffixes if f"player{sfx}" in result.columns]
    stat_cols = [f"stat{sfx}" for sfx in suffixes if f"stat{sfx}" in result.columns]
    result["player"] = result[player_cols[0]].copy()
    for col in player_cols[1:]:
        result["player"] = result["player"].combine_first(result[col])
    result["stat"] = result[stat_cols[0]].copy()
    for col in stat_cols[1:]:
        result["stat"] = result["stat"].combine_first(result[col])

    keep = ["player", "stat", "_join_player", "_join_stat", "threshold", "over_prob", "under_prob"]
    if has_ticker and "ticker" in result.columns:
        keep.append("ticker")
    return result[keep]


def load_underdog(path: str = UNDERDOG_CSV) -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame()
    return pd.read_csv(path)


def find_picks(legs: int, payout: float, source: str = "kalshi",
               min_edge: float = 0.0, standard: bool = False,
               debug: bool = False) -> pd.DataFrame:
    """Return Underdog legs where the source-implied probability beats break-even."""
    probs = load_probs(source)
    if probs.empty:
        cmd = source if source != "both" else "draftkings` and `uv run pinnacle"
        print(f"No {source} probability data found. Run `uv run {cmd}` first (or use --refresh).")
        sys.exit(1)

    underdog = load_underdog()
    if underdog.empty:
        print("No Underdog data. Run `uv run underdog` first (or use --refresh).")
        sys.exit(1)

    if debug:
        print(f"[debug] Source ({source}) rows: {len(probs)}")
        print(f"[debug] Underdog rows: {len(underdog)}")
        print(f"[debug] Underdog columns: {list(underdog.columns)}")
        print(f"[debug] Source stats: {sorted(probs['_join_stat'].dropna().unique())}")
        if "stat_name" in underdog.columns:
            print(f"[debug] Underdog stat_names: {sorted(underdog['stat_name'].dropna().unique())}")

    if "payout_multiplier" not in underdog.columns:
        print("ERROR: 'payout_multiplier' column missing from Underdog data.")
        print("Re-run `uv run underdog` to refresh.")
        sys.exit(1)

    base_be = base_breakeven(legs, payout)

    ud = underdog.copy()
    ud["threshold"] = (ud["stat_value"] + 0.5).round(0).astype(int)
    ud["_join_player"] = ud["full_name"].apply(normalize_player_name)
    ud["_join_stat"] = ud["stat_name"].str.lower().str.strip()

    has_matchup = "matchup" in ud.columns
    over_ud_cols = ["_join_player", "_join_stat", "threshold", "payout_multiplier"]
    if has_matchup:
        over_ud_cols.append("matchup")

    over_ud = (
        ud[ud["choice"] == "over"][over_ud_cols]
        .rename(columns={"payout_multiplier": "ud_over_mult"})
    )
    under_ud = (
        ud[ud["choice"] == "under"][["_join_player", "_join_stat", "threshold", "payout_multiplier"]]
        .rename(columns={"payout_multiplier": "ud_under_mult"})
    )
    ud_pivot = over_ud.merge(under_ud, on=["_join_player", "_join_stat", "threshold"], how="outer")

    if debug:
        print(f"[debug] Underdog pivot rows: {len(ud_pivot)}")

    # Pass 1: exact threshold match
    joined = probs.merge(ud_pivot, on=["_join_player", "_join_stat", "threshold"], how="inner")
    joined["_threshold_adj"] = False

    # Pass 2: Poisson-adjusted fuzzy fallback for UD rows that didn't match
    # (e.g. UD at 14.5 → threshold 15, source at 15.5 → threshold 16)
    unmatched_ud = ud_pivot[
        ~ud_pivot.set_index(["_join_player", "_join_stat", "threshold"]).index.isin(
            joined.set_index(["_join_player", "_join_stat", "threshold"]).index
        )
    ]
    if not unmatched_ud.empty:
        # Build a lookup from the source probs by (player, stat)
        prob_by_key: dict[tuple, pd.DataFrame] = {}
        for (jp, js), grp in probs.groupby(["_join_player", "_join_stat"]):
            prob_by_key[(jp, js)] = grp

        fuzzy_rows = []
        for _, ud_row in unmatched_ud.iterrows():
            jp = ud_row["_join_player"]
            js = ud_row["_join_stat"]
            ud_thresh = int(ud_row["threshold"])

            src_grp = prob_by_key.get((jp, js))
            if src_grp is None:
                continue

            src_grp = src_grp.copy()
            src_grp["_diff"] = (src_grp["threshold"].astype(int) - ud_thresh).abs()
            close = src_grp[src_grp["_diff"] <= 1]
            if close.empty:
                continue

            best = close.loc[close["_diff"].idxmin()]
            src_thresh = int(best["threshold"])
            if src_thresh == ud_thresh:
                continue  # exact match would have been caught above

            # Adjust source probabilities from src_thresh to ud_thresh.
            # Thresholds here are already integers (ceil of stat_value + 0.5),
            # so pass them directly — math.ceil(int) == int.
            adj_over = adjust_prob_for_threshold(
                float(best["over_prob"]) / 100.0, src_thresh, ud_thresh
            )
            if adj_over is None:
                continue
            adj_under = 1.0 - adj_over

            row = best.to_dict()
            row["over_prob"] = round(adj_over * 100, 1)
            row["under_prob"] = round(adj_under * 100, 1)
            row["threshold"] = ud_thresh
            row["ud_over_mult"] = ud_row["ud_over_mult"]
            row["ud_under_mult"] = ud_row["ud_under_mult"]
            row["_threshold_adj"] = True
            if has_matchup:
                row["matchup"] = ud_row.get("matchup")
            fuzzy_rows.append(row)

        if fuzzy_rows:
            fuzzy_df = pd.DataFrame(fuzzy_rows)
            joined = pd.concat([joined, fuzzy_df], ignore_index=True)

    joined = joined.drop(columns=["_join_player", "_join_stat"], errors="ignore")

    if debug:
        print(f"[debug] Rows after join: {len(joined)}")
        if joined.empty:
            print(f"[debug] Sample source (_join_stat, threshold):")
            print(probs[["_join_stat", "threshold"]].drop_duplicates().head(10).to_string(index=False))
            print(f"[debug] Sample Underdog (_join_stat, threshold):")
            print(ud_pivot[["_join_stat", "threshold"]].drop_duplicates().head(10).to_string(index=False))

    has_ticker = "ticker" in joined.columns

    picks = []
    has_matchup = "matchup" in joined.columns

    for _, row in joined.iterrows():
        threshold_label = f"{int(row['threshold']) - 0.5:g}"
        ticker = row["ticker"] if has_ticker else ""
        matchup = row.get("matchup", "") if has_matchup else ""
        prob_adj = bool(row.get("_threshold_adj", False))

        for side, prob_col, mult_col in [
            ("over", "over_prob", "ud_over_mult"),
            ("under", "under_prob", "ud_under_mult"),
        ]:
            prob = row.get(prob_col)
            mult = row.get(mult_col)
            if pd.isna(prob):
                continue

            if standard:
                if not _is_standard_mult(mult):
                    continue
                edge = prob - base_be
                entry = {
                    "player": row.get("player", row.get("full_name", "")),
                    "matchup": matchup,
                    "stat": row.get("stat", row.get("stat_name", "")),
                    "threshold": threshold_label,
                    "ud_pick": side,
                    "prob": prob,
                    "required_prob": round(base_be, 1),
                    "edge": round(edge, 1),
                    "prob_adj": prob_adj,
                }
            else:
                if pd.isna(mult) or mult <= 0:
                    continue
                req = required_prob(base_be, mult)
                edge = prob - req
                entry = {
                    "player": row.get("player", row.get("full_name", "")),
                    "matchup": matchup,
                    "stat": row.get("stat", row.get("stat_name", "")),
                    "threshold": threshold_label,
                    "ud_pick": side,
                    "ud_mult": round(mult, 3),
                    "prob": prob,
                    "required_prob": round(req, 1),
                    "edge": round(edge, 1),
                    "prob_adj": prob_adj,
                }
            if has_ticker:
                entry["ticker"] = ticker
            picks.append(entry)

    if debug:
        print(f"[debug] Picks before edge filter: {len(picks)}")

    if not picks:
        return pd.DataFrame()

    df = pd.DataFrame(picks)

    # Restore meaningful player/stat names from prob source if they came through blank
    if "player" in df.columns:
        df["player"] = df["player"].fillna("")
    if "stat" in df.columns:
        df["stat"] = df["stat"].fillna("")

    df = df[df["edge"] >= min_edge].sort_values("edge", ascending=False).reset_index(drop=True)
    return df


def refresh_data(source: str = "kalshi"):
    from src.underdog import fetch_underdog_data, parse_nba_props

    if source in ("kalshi", "both"):
        from src.main import get_client, fetch_nba_player_props
        print("Refreshing Kalshi NBA player props...")
        client = get_client()
        kalshi_df = fetch_nba_player_props(client)
        kalshi_df.to_csv(KALSHI_CSV, index=False)
        print(f"  Saved {len(kalshi_df)} rows to {KALSHI_CSV}")

    if source in ("draftkings", "both"):
        from src.draftkings import fetch_subcategory, parse_props, SUBCATEGORY_MAP
        print("Refreshing DraftKings NBA player props...")
        all_rows = []
        for subcategory_id, stat_name in SUBCATEGORY_MAP.items():
            try:
                data = fetch_subcategory(subcategory_id)
                all_rows.extend(parse_props(data, stat_name))
            except Exception as e:
                print(f"  {stat_name} (subcategory {subcategory_id}): error — {e}")
        dk_df = pd.DataFrame(all_rows)
        dk_df.to_csv(DRAFTKINGS_CSV, index=False)
        if len(dk_df) == 0:
            print("  WARNING: DraftKings returned 0 rows — API may be down or markets not yet posted.")
        else:
            print(f"  Saved {len(dk_df)} rows to {DRAFTKINGS_CSV}")

    if source in ("pinnacle", "both"):
        from src.pinnacle import fetch_matchups, parse_props as pinnacle_parse_props, fetch_all_prices
        from src.common import KALSHI_STATS
        from src.pinnacle import CATEGORY_MAP
        print("Refreshing Pinnacle NBA player props...")
        matchups = fetch_matchups()
        props = pinnacle_parse_props(matchups)
        if not props.empty:
            props["stat_name"] = props["category"].map(CATEGORY_MAP)
            props = props.dropna(subset=["stat_name"])
            props = props[props["stat_name"].isin(KALSHI_STATS)]
        if props.empty:
            print("  WARNING: Pinnacle returned 0 rows — API may be down or markets not yet posted.")
        else:
            matchup_ids = props["matchup_id"].unique().tolist()
            prices = fetch_all_prices(matchup_ids)
            if not prices.empty:
                from src.pinnacle import american_to_decimal
                merged = props.merge(prices, on="participant_id", how="inner")
                merged["odds_decimal"] = merged["odds_american"].apply(american_to_decimal)
                result = merged[["player", "stat_name", "stat_value", "choice", "odds_decimal", "odds_american"]].copy()
                result = result.rename(columns={"player": "full_name"})
                result.to_csv(PINNACLE_CSV, index=False)
                print(f"  Saved {len(result)} rows to {PINNACLE_CSV}")
            else:
                print("  WARNING: No Pinnacle prices found.")

    print("Refreshing Underdog Fantasy NBA player props...")
    data = fetch_underdog_data()
    ud_df = parse_nba_props(data)
    ud_df.to_csv(UNDERDOG_CSV, index=False)
    print(f"  Saved {len(ud_df)} rows to {UNDERDOG_CSV}")


def load_previous_pick_keys(path: str = UNDERDOG_PICKS_CSV) -> set[tuple]:
    """Return (player, stat, threshold, ud_pick) tuples from the last saved run."""
    if not os.path.exists(path):
        return set()
    try:
        df = pd.read_csv(path)
        if df.empty:
            return set()
        cols = ["player", "stat", "threshold", "ud_pick"]
        if not all(c in df.columns for c in cols):
            return set()
        return set(zip(df["player"], df["stat"], df["threshold"].astype(str), df["ud_pick"]))
    except Exception:
        return set()


def render_picks_image(df: pd.DataFrame) -> bytes:
    """Render the picks DataFrame as a PNG image and return raw bytes.

    Columns: new marker, player, matchup, stat, line, pick, mult, prob%, req%, edge.
    NEW rows are highlighted green; numeric columns are right-aligned; prob/edge
    values are formatted to one decimal place.
    """
    import io
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    col_spec: list[tuple[str, str, str]] = [
        # (src_col, header, align)
        ("player",    "player",  "l"),
        ("stat",      "stat",    "l"),
        ("ud_pick",   "pick",    "l"),
        ("threshold", "line",    "r"),
        ("matchup",   "matchup", "l"),
        ("ud_mult",   "mult",    "r"),
        ("edge",      "edge",    "r"),
        ("prob_adj",  "adj",     "l"),
    ]
    spec = [(src, hdr, aln) for src, hdr, aln in col_spec if src in df.columns]
    numeric_srcs = {"threshold", "ud_mult", "edge"}
    right_cols = {i for i, (src, _, _) in enumerate(spec) if src in numeric_srcs}

    def fmt(col: str, val) -> str:
        if val is None or (isinstance(val, float) and pd.isna(val)) or val == "":
            return ""
        if col == "edge":
            return f"{float(val):.1f}"
        if col == "ud_mult":
            return f"{float(val):.2f}"
        if col == "prob_adj":
            return "" if val is True or str(val).lower() == "true" else "-"
        return str(val)

    headers = [hdr for _, hdr, _ in spec]
    cell_data = [
        [fmt(src, row.get(src, "")) for src, _, _ in spec]
        for _, row in df.iterrows()
    ]
    n_rows, n_cols = len(cell_data), len(spec)

    HEADER_BG   = "#1a1a2e"
    HEADER_FG   = "#ffffff"
    OVER_BG     = "#d4edda"
    UNDER_BG    = "#f8d7da"
    ROW_BG_EVEN = "#ffffff"
    ROW_BG_ODD  = "#f2f2f2"
    EDGE_FG     = "#dddddd"

    row_colors = []
    for i, (_, row) in enumerate(df.iterrows()):
        pick = str(row.get("ud_pick", "")).lower()
        if pick == "over":
            row_colors.append([OVER_BG] * n_cols)
        elif pick == "under":
            row_colors.append([UNDER_BG] * n_cols)
        elif i % 2 == 0:
            row_colors.append([ROW_BG_EVEN] * n_cols)
        else:
            row_colors.append([ROW_BG_ODD] * n_cols)

    fig_w = max(18, n_cols * 2.5)
    fig_h = (n_rows + 1) * 0.65 + 0.6
    fig, ax = plt.subplots(figsize=(fig_w, fig_h))
    fig.patch.set_facecolor("#ffffff")
    ax.set_facecolor("#ffffff")
    ax.axis("off")

    tbl = ax.table(
        cellText=cell_data,
        colLabels=headers,
        cellLoc="left",
        loc="upper left",
        bbox=[0, 0, 1, 1],
        cellColours=row_colors,
    )

    # Header styling
    for j in range(n_cols):
        cell = tbl[0, j]
        cell.set_facecolor(HEADER_BG)
        cell.set_text_props(color=HEADER_FG, fontweight="bold", fontsize=20)
        cell.set_edgecolor(HEADER_BG)

    new_rows = {i + 1 for i, (_, row) in enumerate(df.iterrows()) if str(row.get("new", "")) == "NEW"}

    # Data cell styling
    for (r, c), cell in tbl.get_celld().items():
        if r == 0:
            continue
        cell.set_edgecolor(EDGE_FG)
        weight = "bold" if r in new_rows else "normal"
        cell.set_text_props(fontsize=19, fontweight=weight)
        cell.get_text().set_ha("right" if c in right_cols else "left")

    tbl.auto_set_font_size(False)
    tbl.auto_set_column_width(range(n_cols))

    buf = io.BytesIO()
    plt.savefig(buf, format="png", dpi=200, bbox_inches="tight",
                facecolor=fig.get_facecolor())
    plt.close(fig)
    buf.seek(0)
    return buf.read()


def send_to_slack(image_bytes: bytes, token: str, channel: str) -> None:
    """Upload a picks PNG to a Slack channel via the Web API."""
    import io
    from slack_sdk import WebClient
    from slack_sdk.errors import SlackApiError

    client = WebClient(token=token)
    try:
        client.files_upload_v2(
            channel=channel,
            file=io.BytesIO(image_bytes),
            filename="picks.png",
            title="Underdog Picks",
        )
    except SlackApiError as e:
        print(f"Failed to send Slack notification: {e.response['error']}")


def main():
    parser = argparse.ArgumentParser(
        description="Find +EV Underdog Fantasy legs using an external probability source.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Default: Kalshi as source, 2-leg at 3x
  uv run ud-picks

  # Use DraftKings as the probability source
  uv run ud-picks --source draftkings

  # Average Kalshi + DraftKings
  uv run ud-picks --source both

  # Standard 3.5x flat lines only, using DraftKings
  uv run ud-picks --standard --source draftkings

  # Refresh all data first
  uv run ud-picks --source draftkings --refresh
        """,
    )
    parser.add_argument("--source", default="kalshi", choices=["kalshi", "draftkings", "pinnacle", "both"],
                        help="Probability source (default: kalshi)")
    parser.add_argument("--standard", action="store_true",
                        help="Only show standard 1.0x lines (flat 3.5x two-pick payout). "
                             "Sets --payout 3.5 by default.")
    parser.add_argument("--legs", type=int, default=2,
                        help="Number of legs in the entry (default: 2)")
    parser.add_argument("--payout", type=float, default=None,
                        help="Base payout multiplier (default: 3.5 with --standard, 3.0 otherwise)")
    parser.add_argument("--min-edge", type=float, default=0.0,
                        help="Minimum edge in percentage points to show (default: 0)")
    parser.add_argument("--top", type=int, default=None,
                        help="Show top N picks (default: all in --standard mode, 20 otherwise)")
    parser.add_argument("--refresh", action="store_true",
                        help="Re-fetch data before scanning")
    parser.add_argument("--save", action="store_true",
                        help=f"Save all results to {UNDERDOG_PICKS_CSV}")
    parser.add_argument("--archive", action="store_true",
                        help="Upload source CSVs and picks to S3 (requires S3_BUCKET env var)")
    parser.add_argument("--debug", action="store_true",
                        help="Print diagnostic info about row counts and join keys")
    parser.add_argument("--slack", action="store_true",
                        help="Post picks to Slack via SLACK_BOT_TOKEN + SLACK_CHANNEL_ID env vars")
    args = parser.parse_args()

    payout = args.payout if args.payout is not None else (3.5 if args.standard else 3.0)
    top = args.top if args.top is not None else (None if args.standard else 20)

    if args.refresh:
        refresh_data(source=args.source)
        print()

    base_be = base_breakeven(args.legs, payout)
    if args.standard:
        print(f"Standard mode — {args.legs}-leg flat entry @ {payout}x  |  source: {args.source}")
        print(f"Showing all lines where implied prob > {base_be:.1f}% (payout_multiplier == 1.0)\n")
    else:
        print(f"{args.legs}-leg entry @ {payout}x  |  source: {args.source}")
        print(f"Base break-even (1.0x leg): {base_be:.1f}%")
        print(f"  e.g. 0.75x leg needs {base_be/0.75:.1f}%  |  1.1x leg needs {base_be/1.1:.1f}%")
        print(f"Scanning for picks with edge >= {args.min_edge}pp...\n")

    prev_keys = load_previous_pick_keys()

    picks = find_picks(
        legs=args.legs, payout=payout, source=args.source,
        min_edge=args.min_edge, standard=args.standard, debug=args.debug,
    )

    if picks.empty:
        print("No picks found above their required probability.")
        return

    # Mark picks that weren't in the last saved run
    if prev_keys:
        picks["new"] = picks.apply(
            lambda r: (r["player"], r["stat"], str(r["threshold"]), r["ud_pick"]) not in prev_keys,
            axis=1,
        )
        # Sort: new picks first, then by edge descending
        picks = picks.sort_values(["new", "edge"], ascending=[False, False]).reset_index(drop=True)
        new_count = picks["new"].sum()
    else:
        picks["new"] = False
        new_count = 0

    display = picks if top is None else picks.head(top)
    label = f"top {len(display)}" if top is not None else "all"

    if prev_keys:
        print(f"Found {len(picks)} pick(s) | showing {label} | {new_count} new since last run:\n")
    else:
        print(f"Found {len(picks)} pick(s) | showing {label}:\n")

    # Replace boolean with readable marker, insert as first column
    display = display.copy()
    display["new"] = display["new"].map({True: "NEW", False: ""})
    cols = ["new"] + [c for c in display.columns if c != "new"]
    table_str = display[cols].to_string(index=False)
    print(table_str)

    if not args.standard:
        print("\nColumns: ud_mult = Underdog payout multiplier for this leg")
        print("         required_prob = break-even accounting for ud_mult")
        print("         edge = prob - required_prob (positive = +EV pick)")

    if args.save:
        picks.drop(columns=["new"], errors="ignore").to_csv(UNDERDOG_PICKS_CSV, index=False)
        print(f"\nSaved {len(picks)} picks to {UNDERDOG_PICKS_CSV}")

    if args.archive:
        from src.storage import upload_run_snapshot
        upload_run_snapshot(
            source=args.source,
            picks=picks.drop(columns=["new"], errors="ignore"),
        )

    if args.slack:
        from dotenv import load_dotenv
        load_dotenv()
        token = os.getenv("SLACK_BOT_TOKEN")
        channel = os.getenv("SLACK_CHANNEL_ID")
        if not token or not channel:
            print("SLACK_BOT_TOKEN and SLACK_CHANNEL_ID must be set — skipping Slack notification.")
        else:
            send_to_slack(render_picks_image(display), token, channel)
            print("Sent picks to Slack.")


if __name__ == "__main__":
    main()
