import requests
import pandas as pd
from urllib.parse import quote

from src.common import CBB_DRAFTKINGS_CSV

BASE_URL = "https://sportsbook-nash.draftkings.com/sites/US-SB/api/sportscontent/controldata/league/leagueSubcategory/v1/markets"
LEAGUE_ID = "92483"  # College Basketball (M) / NCAAB
SUBCATEGORY_ID = 4511  # Game Lines (includes Moneyline, Spread, Total)

HEADERS = {
    "authority": "sportsbook-nash.draftkings.com",
    "accept": "*/*",
    "accept-language": "en-US,en;q=0.9",
    "origin": "https://sportsbook.draftkings.com",
    "referer": "https://sportsbook.draftkings.com/",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
}


def build_url() -> str:
    events_q = f"$filter=leagueId eq '{LEAGUE_ID}' AND clientMetadata/Subcategories/any(s: s/Id eq '{SUBCATEGORY_ID}')"
    markets_q = f"$filter=clientMetadata/subCategoryId eq '{SUBCATEGORY_ID}' AND tags/all(t: t ne 'SportcastBetBuilder')"
    return (
        f"{BASE_URL}?isBatchable=false"
        f"&templateVars={LEAGUE_ID}%2C{SUBCATEGORY_ID}"
        f"&eventsQuery={quote(events_q)}"
        f"&marketsQuery={quote(markets_q)}"
        f"&include=Events&entity=event"
    )


def fetch_game_totals() -> dict:
    url = build_url()
    response = requests.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()
    return response.json()


def parse_totals(data: dict) -> list[dict]:
    markets = data.get("markets", [])
    selections = data.get("selections", [])
    events = data.get("events", [])
    if not markets or not selections:
        return []

    events_by_id = {e["id"]: e for e in events}
    markets_by_id = {}
    for m in markets:
        # Filter to Total markets only (skip Moneyline and Spread)
        market_type = m.get("marketType", {})
        if market_type.get("name") != "Total":
            continue
        markets_by_id[m["id"]] = m

    rows = []
    for sel in selections:
        market = markets_by_id.get(sel.get("marketId"))
        if not market:
            continue

        label = sel.get("label", "")
        if label not in ("Over", "Under"):
            continue

        # Get the game name from the event
        event_id = market.get("eventId")
        event = events_by_id.get(event_id, {})
        game_name = event.get("name", "")

        display_odds = sel.get("displayOdds", {})

        rows.append({
            "game": game_name,
            "total_line": sel.get("points"),
            "choice": label.lower(),
            "odds_decimal": sel.get("trueOdds"),
            "odds_american": display_odds.get("american", "").replace("\u2212", "-"),
        })

    return rows


def main():
    print("Fetching DraftKings NCAAB game totals...")
    try:
        data = fetch_game_totals()
        rows = parse_totals(data)
    except Exception as e:
        print(f"Error fetching DraftKings CBB totals: {e}")
        return

    df = pd.DataFrame(rows)
    if len(df) > 0:
        df.to_csv(CBB_DRAFTKINGS_CSV, index=False)
    print(f"Found {len(df)} NCAAB game total lines")
    print(f"Saved to {CBB_DRAFTKINGS_CSV}")


if __name__ == "__main__":
    main()
