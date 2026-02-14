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


def parse_kalshi_title(title):
    """Extract player name and threshold from a Kalshi market title.

    Example: 'Victor Wembanyama: 35+ points' -> ('Victor Wembanyama', 35)
    """
    match = re.match(r"^(.+?):\s*(\d+)\+\s+\w+", title)
    if not match:
        return None, None
    return match.group(1).strip(), int(match.group(2))
