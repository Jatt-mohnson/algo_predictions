import requests
import pandas as pd

from src.common import CBB_PINNACLE_CSV
from src.pinnacle import american_to_decimal

BASE_URL = "https://guest.api.arcadia.pinnacle.com/0.1"
LEAGUE_ID = 493  # NCAAB

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "x-api-key": "CmX2KcMrXuFmNg6YFbmTxE0y9CIrOi0R",
}


def fetch_matchups() -> list[dict]:
    url = f"{BASE_URL}/leagues/{LEAGUE_ID}/matchups"
    response = requests.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()
    return response.json()


def fetch_prices(matchup_id: int) -> list[dict]:
    url = f"{BASE_URL}/matchups/{matchup_id}/markets/straight"
    response = requests.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()
    return response.json()


def fetch_related_prices(matchup_id: int) -> list[dict]:
    """Fetch all markets including alternate lines for a matchup."""
    url = f"{BASE_URL}/matchups/{matchup_id}/markets/related/straight"
    response = requests.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()
    return response.json()


def parse_game_totals(matchups: list[dict]) -> pd.DataFrame:
    """Parse matchups to find game total (over/under) markets."""
    rows = []
    for m in matchups:
        # Game totals are on the main matchup (not special/props)
        if m.get("special"):
            continue
        if not m.get("participants") or len(m["participants"]) < 2:
            continue

        # Build game name from participants
        home = away = None
        for p in m["participants"]:
            alignment = p.get("alignment")
            if alignment == "home":
                home = p["name"]
            elif alignment == "away":
                away = p["name"]
        if not home or not away:
            continue

        game_name = f"{away} @ {home}"
        matchup_id = m["id"]

        rows.append({
            "matchup_id": matchup_id,
            "game": game_name,
        })

    return pd.DataFrame(rows)


def fetch_total_prices(matchup_ids: list[int], include_alternates=False) -> pd.DataFrame:
    """Fetch total (over/under) prices for the given matchup IDs.

    If include_alternates is True, uses the /related/straight endpoint
    to get all alternate total lines (not just the main line).
    """
    rows = []
    fetch_fn = fetch_related_prices if include_alternates else fetch_prices
    for mid in matchup_ids:
        try:
            data = fetch_fn(mid)
        except Exception as e:
            print(f"  Error fetching prices for matchup {mid}: {e}")
            continue
        for market in data:
            market_type = market.get("type", "")
            if market_type != "total":
                continue
            # Only include full-game totals (period 0)
            if market.get("period", 0) != 0:
                continue
            for price in market.get("prices", []):
                designation = price.get("designation", "")
                if designation not in ("over", "under"):
                    continue
                rows.append({
                    "matchup_id": mid,
                    "total_line": price.get("points"),
                    "choice": designation,
                    "odds_american": price.get("price"),
                })
    return pd.DataFrame(rows)


def main():
    print("Fetching Pinnacle NCAAB game totals...")

    matchups = fetch_matchups()
    games = parse_game_totals(matchups)
    if games.empty:
        print("No NCAAB game totals found.")
        return

    matchup_ids = games["matchup_id"].unique().tolist()
    print(f"  Fetching prices (with alternate lines) for {len(matchup_ids)} games...")
    prices = fetch_total_prices(matchup_ids, include_alternates=True)

    if prices.empty:
        print("No total prices found.")
        return

    merged = games.merge(prices, on="matchup_id", how="inner")
    merged["odds_decimal"] = merged["odds_american"].apply(american_to_decimal)

    result = merged[["game", "total_line", "choice", "odds_decimal", "odds_american"]].copy()

    result.to_csv(CBB_PINNACLE_CSV, index=False)
    print(f"Found {len(result)} NCAAB game total lines")
    print(f"Saved to {CBB_PINNACLE_CSV}")


if __name__ == "__main__":
    main()
