import os

import pandas as pd
from dotenv import load_dotenv
from pykalshi import KalshiClient, MarketStatus, to_dataframe

load_dotenv()

NBA_PLAYER_PROP_SERIES = [
    "KXNBAPTS",  # Points
    "KXNBAREB",  # Rebounds
    "KXNBAAST",  # Assists
    "KXNBA3PT",  # Three-pointers made
    "KXNBASTL",  # Steals
    "KXNBABLK",  # Blocks
]


def get_client() -> KalshiClient:
    api_key_id = os.getenv("KALSHI_API_KEY_ID")
    private_key_path = os.getenv("KALSHI_PRIVATE_KEY_PATH")

    if not api_key_id or not private_key_path:
        raise ValueError(
            "Set KALSHI_API_KEY_ID and KALSHI_PRIVATE_KEY_PATH in your .env file. "
            "See .env.example for reference."
        )

    return KalshiClient(api_key_id=api_key_id, private_key_path=private_key_path)


def fetch_nba_player_props(client: KalshiClient) -> pd.DataFrame:
    all_markets = []
    for series in NBA_PLAYER_PROP_SERIES:
        markets = client.get_markets(
            series_ticker=series, status=MarketStatus.OPEN, fetch_all=True
        )
        all_markets.extend(markets)

    return to_dataframe(all_markets)


def main():
    client = get_client()

    print("Fetching NBA player prop markets...")
    df = fetch_nba_player_props(client)
    df.to_csv("nba_player_props.csv", index=False)
    print(f"Found {len(df)} open player prop markets")


if __name__ == "__main__":
    main()
