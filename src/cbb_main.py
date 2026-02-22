import pandas as pd
from pykalshi import MarketStatus, to_dataframe

from src.common import CBB_KALSHI_CSV, parse_cbb_ticker
from src.main import get_client

CBB_SERIES = "KXNCAAMBTOTAL"


def fetch_cbb_totals(client) -> pd.DataFrame:
    markets = client.get_markets(
        series_ticker=CBB_SERIES, status=MarketStatus.OPEN, fetch_all=True
    )
    return to_dataframe(markets)


def main():
    client = get_client()

    print("Fetching Kalshi CBB total markets...")
    df = fetch_cbb_totals(client)

    if df.empty:
        print("No open CBB total markets found.")
        return

    # Parse tickers to extract team info and threshold
    parsed = df["ticker"].apply(lambda t: pd.Series(parse_cbb_ticker(t), index=["team1", "team2", "threshold_parsed"]))
    df = pd.concat([df, parsed], axis=1)

    df.to_csv(CBB_KALSHI_CSV, index=False)
    print(f"Found {len(df)} open CBB total markets")
    print(f"Saved to {CBB_KALSHI_CSV}")


if __name__ == "__main__":
    main()
