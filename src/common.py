"""Shared constants and utilities used across the project."""

import os
import re

# Project root is the parent of the src/ directory
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(_PROJECT_ROOT, "data")

# Map Kalshi series tickers to stat names
SERIES_TO_STAT = {
    "KXNBAPTS": "Points",
    "KXNBAREB": "Rebounds",
    "KXNBAAST": "Assists",
    "KXNBA3PT": "3-Pointers Made",
    "KXNBASTL": "Steals",
    "KXNBABLK": "Blocks",
}

# Stats that Kalshi supports (used to filter sportsbook data)
KALSHI_STATS = set(SERIES_TO_STAT.values())

# CSV output paths (all under data/)
KALSHI_CSV = os.path.join(DATA_DIR, "nba_player_props.csv")
DRAFTKINGS_CSV = os.path.join(DATA_DIR, "draftkings_nba_props.csv")
PINNACLE_CSV = os.path.join(DATA_DIR, "pinnacle_nba_props.csv")
UNDERDOG_CSV = os.path.join(DATA_DIR, "underdog_nba_props.csv")
EDGES_CSV = os.path.join(DATA_DIR, "edges.csv")
TRADES_LOG_CSV = os.path.join(DATA_DIR, "trades_log.csv")
COMBINED_CSV = os.path.join(DATA_DIR, "combined_odds.csv")

# CBB totals CSV paths
CBB_KALSHI_CSV = os.path.join(DATA_DIR, "cbb_totals.csv")
CBB_DRAFTKINGS_CSV = os.path.join(DATA_DIR, "cbb_draftkings.csv")
CBB_PINNACLE_CSV = os.path.join(DATA_DIR, "cbb_pinnacle.csv")
CBB_EDGES_CSV = os.path.join(DATA_DIR, "cbb_edges.csv")


# Known nickname/alias → canonical name mappings for cross-source player matching.
# Add entries here whenever a player's name differs between sources.
# All keys and values must be lowercase.
PLAYER_ALIASES: dict[str, str] = {
    "herb jones": "herbert jones",
    "bub carrington": "carlton carrington",
}


def normalize_player_name(name: str) -> str:
    """Normalize a player name for cross-source matching.

    Lowercases and strips whitespace, then resolves known nicknames/aliases
    to a canonical form so joins across data sources succeed.
    """
    key = name.lower().strip()
    return PLAYER_ALIASES.get(key, key)


def parse_kalshi_title(title):
    """Extract player name and threshold from a Kalshi market title.

    Example: 'Victor Wembanyama: 35+ points' -> ('Victor Wembanyama', 35)
    """
    match = re.match(r"^(.+?):\s*(\d+)\+\s+\w+", title)
    if not match:
        return None, None
    return match.group(1).strip(), int(match.group(2))


def parse_cbb_ticker(ticker):
    """Extract teams and threshold from a CBB total ticker.

    Example: 'KXNCAAMBTOTAL-26FEB16SYRDUKE-139' -> ('SYR', 'DUKE', 139)
    Returns (team1, team2, threshold) or (None, None, None) on failure.
    """
    parts = ticker.split("-")
    if len(parts) < 3:
        return None, None, None
    try:
        threshold = int(parts[2])
    except (ValueError, IndexError):
        return None, None, None
    # Middle part is date + two team codes (e.g. 26FEB16SYRDUKE)
    mid = parts[1]
    # Strip leading date portion: digits + month abbrev + digits (e.g. 26FEB16)
    match = re.match(r"^\d{2}[A-Z]{3}\d{2}(.+)$", mid)
    if not match:
        return None, None, None
    teams_str = match.group(1)
    # Teams are concatenated uppercase codes — split by finding where a
    # reasonable midpoint is. Kalshi uses 2-5 char team codes.
    # We'll try splitting at every position and pick a reasonable split.
    # Common approach: teams are typically 3-5 chars each.
    best = None
    for i in range(2, len(teams_str) - 1):
        t1, t2 = teams_str[:i], teams_str[i:]
        if 2 <= len(t1) <= 6 and 2 <= len(t2) <= 6:
            best = (t1, t2)
            break
    if not best:
        return teams_str, "", threshold
    return best[0], best[1], threshold
