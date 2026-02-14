import requests
import pandas as pd

from src.common import KALSHI_STATS, PINNACLE_CSV

BASE_URL = "https://guest.api.arcadia.pinnacle.com/0.1"
LEAGUE_ID = 487  # NBA

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "x-api-key": "CmX2KcMrXuFmNg6YFbmTxE0y9CIrOi0R",
}

# Map Pinnacle category names to Kalshi stat names
CATEGORY_MAP = {
    "Points": "Points",
    "Rebounds": "Rebounds",
    "Assists": "Assists",
    "3-Point Field Goals": "3-Pointers Made",
    "3 Point FG": "3-Pointers Made",
    "Steals": "Steals",
    "Blocked Shots": "Blocks",
    "Blocks": "Blocks",
}


def american_to_decimal(american: float) -> float:
    if american >= 100:
        return round(1 + american / 100, 4)
    elif american <= -100:
        return round(1 + 100 / abs(american), 4)
    return None


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


def parse_props(matchups: list[dict]) -> pd.DataFrame:
    rows = []
    for m in matchups:
        if not m.get("parent") or not m.get("special"):
            continue
        description = m["special"].get("description", "")
        if "(" not in description or ")" not in description:
            continue

        player = description.split("(")[0].strip()
        category = description.split("(")[1].split(")")[0].strip()

        for participant in m.get("participants", []):
            name = participant.get("name", "")
            if name in ("Over", "Under"):
                rows.append({
                    "matchup_id": m["id"],
                    "participant_id": participant["id"],
                    "player": player,
                    "category": category,
                    "choice": name.lower(),
                })
    return pd.DataFrame(rows)


def fetch_all_prices(matchup_ids: list[int]) -> pd.DataFrame:
    rows = []
    for mid in matchup_ids:
        try:
            data = fetch_prices(mid)
        except Exception as e:
            print(f"  Error fetching prices for matchup {mid}: {e}")
            continue
        for market in data:
            for price in market.get("prices", []):
                rows.append({
                    "participant_id": price["participantId"],
                    "odds_american": price.get("price"),
                    "stat_value": price.get("points"),
                })
    return pd.DataFrame(rows)


def main():
    print("Fetching Pinnacle NBA player props...")

    matchups = fetch_matchups()
    props = parse_props(matchups)
    if props.empty:
        print("No props found.")
        return

    # Map categories and filter to Kalshi-supported stats
    props["stat_name"] = props["category"].map(CATEGORY_MAP)
    props = props.dropna(subset=["stat_name"])
    props = props[props["stat_name"].isin(KALSHI_STATS)]

    if props.empty:
        print("No Kalshi-supported props found.")
        return

    matchup_ids = props["matchup_id"].unique().tolist()
    print(f"  Fetching prices for {len(matchup_ids)} prop matchups...")
    prices = fetch_all_prices(matchup_ids)

    if prices.empty:
        print("No prices found.")
        return

    merged = props.merge(prices, on="participant_id", how="inner")
    merged["odds_decimal"] = merged["odds_american"].apply(american_to_decimal)

    result = merged[["player", "stat_name", "stat_value", "choice", "odds_decimal", "odds_american"]].copy()
    result = result.rename(columns={"player": "full_name"})

    for stat in sorted(result["stat_name"].unique()):
        count = len(result[result["stat_name"] == stat])
        print(f"  {stat}: {count} lines")

    result.to_csv(PINNACLE_CSV, index=False)
    print(f"Found {len(result)} NBA player prop lines")
    print(f"Saved to {PINNACLE_CSV}")


if __name__ == "__main__":
    main()
