"""Shared constants and utilities used across the project."""

import math
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


# Negative Binomial dispersion parameters (r) fit from 2024-25 NBA game logs,
# filtered to players with >= 15 minutes played. Higher r = less overdispersion.
# Combo stats computed as sum of components in the same game.
NB_DISPERSION: dict[str, float] = {
    "points":           2.82,
    "rebounds":         4.14,
    "assists":          2.44,
    "steals":           5.81,
    "blocks":           1.34,
    "3-pointers made":  2.22,
    "points+rebounds":  4.06,
    "points+assists":   3.30,
    "rebounds+assists": 5.28,
    "pts+rebs+asts":    4.40,
}


def estimate_nb_mu(prob: float, k: int, r: float) -> float | None:
    """Find μ such that P(X >= k) = prob under a NegativeBinomial(r, μ) distribution.

    Uses the scipy nbinom(n=r, p=r/(r+μ)) parameterisation.
    Returns None if the probability is out of range or the solver fails.
    """
    from scipy.optimize import brentq
    from scipy.stats import nbinom

    if not (0.0 < prob < 1.0):
        return None

    def _tail(mu):
        p = r / (r + mu)
        return (1.0 - nbinom.cdf(k - 1, n=r, p=p)) - prob

    try:
        return brentq(_tail, 1e-9, 2000.0, xtol=1e-6)
    except (ValueError, RuntimeError):
        return None


def adjust_prob_for_threshold(
    prob: float,
    from_line: float,
    to_line: float,
    stat: str | None = None,
) -> float | None:
    """Adjust an over-probability from one line to another using a count distribution.

    Uses a Negative Binomial model (parameterised from 2024-25 NBA game logs) when
    a fitted dispersion value is available for the stat; falls back to Poisson otherwise.

    Args:
        prob:       vig-free over-probability (0–1 scale) at from_line.
        from_line:  original line, e.g. 15.5 for "over 15.5" (X ≥ 16).
        to_line:    target line, e.g. 14.5 for "over 14.5" (X ≥ 15).
        stat:       stat name (e.g. "Points", "Rebounds+Assists") used to look up
                    the NB dispersion parameter.  Case-insensitive.  If None or
                    unrecognised, falls back to Poisson.

    Returns:
        Adjusted probability (0–1) at to_line, or None on numerical failure.
    """
    from scipy.stats import nbinom, poisson

    from_k = math.ceil(from_line)  # "over 15.5" → X ≥ 16
    to_k = math.ceil(to_line)      # "over 14.5" → X ≥ 15
    if from_k == to_k:
        return prob

    # Look up NB dispersion for this stat
    r = NB_DISPERSION.get(stat.lower().strip()) if stat else None

    if r is not None:
        mu = estimate_nb_mu(prob, from_k, r)
        if mu is None:
            return None
        p = r / (r + mu)
        return float(1.0 - nbinom.cdf(to_k - 1, n=r, p=p))

    # Fallback: Poisson
    from scipy.optimize import brentq
    if not (0.0 < prob < 1.0):
        return None
    try:
        lam = brentq(
            lambda lam: (1.0 - poisson.cdf(from_k - 1, lam)) - prob,
            1e-9, 500.0, xtol=1e-6,
        )
    except (ValueError, RuntimeError):
        return None
    return float(1.0 - poisson.cdf(to_k - 1, lam))


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
