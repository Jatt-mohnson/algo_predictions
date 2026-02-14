import requests
import pandas as pd
from urllib.parse import quote

from src.common import KALSHI_STATS, DRAFTKINGS_CSV

BASE_URL = "https://sportsbook-nash.draftkings.com/sites/US-SB/api/sportscontent/controldata/league/leagueSubcategory/v1/markets"
LEAGUE_ID = "42648"  # NBA

HEADERS = {
    "authority": "sportsbook-nash.draftkings.com",
    "accept": "*/*",
    "accept-language": "en-US,en;q=0.9",
    "origin": "https://sportsbook.draftkings.com",
    "referer": "https://sportsbook.draftkings.com/",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
}

# Subcategory ID -> Kalshi stat name
SUBCATEGORY_MAP = {
    12488: "Points",
    12492: "Rebounds",
    12495: "Assists",
    12497: "3-Pointers Made",
    12499: "Steals",
    12500: "Blocks",
    12502: "Turnovers",
    12504: "Turnovers",
}


def build_url(subcategory_id: int) -> str:
    events_q = f"$filter=leagueId eq '{LEAGUE_ID}' AND clientMetadata/Subcategories/any(s: s/Id eq '{subcategory_id}')"
    markets_q = f"$filter=clientMetadata/subCategoryId eq '{subcategory_id}' AND tags/all(t: t ne 'SportcastBetBuilder')"
    return (
        f"{BASE_URL}?isBatchable=false"
        f"&templateVars={LEAGUE_ID}%2C{subcategory_id}"
        f"&eventsQuery={quote(events_q)}"
        f"&marketsQuery={quote(markets_q)}"
        f"&include=Events&entity=event"
    )


def fetch_subcategory(subcategory_id: int) -> dict:
    url = build_url(subcategory_id)
    response = requests.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()
    return response.json()


def parse_props(data: dict, stat_name: str) -> list[dict]:
    markets = data.get("markets", [])
    selections = data.get("selections", [])
    if not markets or not selections:
        return []

    markets_by_id = {m["id"]: m for m in markets}
    rows = []

    for sel in selections:
        market = markets_by_id.get(sel.get("marketId"))
        if not market:
            continue

        participants = sel.get("participants", [])
        player_name = participants[0]["name"] if participants else None
        if not player_name:
            continue

        label = sel.get("label", "")
        if label not in ("Over", "Under"):
            continue

        display_odds = sel.get("displayOdds", {})

        rows.append({
            "full_name": player_name,
            "stat_name": stat_name,
            "stat_value": sel.get("points"),
            "choice": label.lower(),
            "odds_decimal": sel.get("trueOdds"),
            "odds_american": display_odds.get("american", "").replace("\u2212", "-"),
        })

    return rows


def main():
    print("Fetching DraftKings NBA player props...")
    all_rows = []

    for subcategory_id, stat_name in SUBCATEGORY_MAP.items():
        if stat_name not in KALSHI_STATS:
            continue
        try:
            data = fetch_subcategory(subcategory_id)
            rows = parse_props(data, stat_name)
            if rows:
                print(f"  {stat_name}: {len(rows)} lines")
            all_rows.extend(rows)
        except Exception as e:
            print(f"  {stat_name} (subcategory {subcategory_id}): error — {e}")

    df = pd.DataFrame(all_rows)
    if len(df) > 0:
        df.to_csv(DRAFTKINGS_CSV, index=False)
    print(f"Found {len(df)} NBA player prop lines")
    print(f"Saved to {DRAFTKINGS_CSV}")


if __name__ == "__main__":
    main()
