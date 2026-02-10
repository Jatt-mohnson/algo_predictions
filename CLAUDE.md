# Algo Predictions

NBA player prop trading system that pulls markets from Kalshi and Underdog Fantasy, detects pricing edges, and places trades on Kalshi.

## Project Structure

- `main.py` — Fetches open NBA player prop markets from the Kalshi API via `pykalshi` and saves to `nba_player_props.csv`
- `underdog.py` — Fetches NBA player prop lines from the Underdog Fantasy public API and saves to `underdog_nba_props.csv`
- `trade.py` — Trading script with manual order placement and automated edge detection between Kalshi and Underdog

## Setup

Requires Python 3.12+. Uses `uv` for dependency management.

```
uv sync
```

Kalshi API credentials go in `.env` (see `.env.example`):
- `KALSHI_API_KEY_ID` — your Kalshi API key ID
- `KALSHI_PRIVATE_KEY_PATH` — path to your private key file (e.g., `./private-key.key`)

## Usage Examples

### Fetch market data

```bash
# Pull current Kalshi NBA player prop markets
uv run python main.py

# Pull current Underdog Fantasy NBA player prop lines
uv run python underdog.py
```

Both scripts write CSV files that `trade.py` reads from. Run these first to get fresh data, or use `--refresh` on the auto subcommand to fetch inline.

### Manual trading

```bash
# Dry run — see what would be placed without executing
uv run python trade.py --dry-run manual \
  --ticker KXNBAPTS-26FEB10SASLAL-SASVWEMBANYAMA1-35 \
  --action buy --side yes --count 1 --price 5 --type limit

# Place a real limit order (will prompt for confirmation)
uv run python trade.py manual \
  --ticker KXNBAPTS-26FEB10SASLAL-SASVWEMBANYAMA1-35 \
  --action buy --side yes --count 1 --price 5 --type limit

# Skip confirmation prompt
uv run python trade.py --yes manual \
  --ticker KXNBAPTS-26FEB10SASLAL-SASVWEMBANYAMA1-35 \
  --action buy --side yes --count 1 --price 5 --type limit
```

### Automated edge detection

```bash
# Scan for edges (dry run) — finds mispriced markets without trading
uv run python trade.py --dry-run auto --min-edge 10

# Refresh data from both sources before scanning
uv run python trade.py --dry-run auto --refresh --min-edge 10

# Trade detected edges with custom guardrails
uv run python trade.py auto --min-edge 8 --count 3 --max-contracts 10 --max-spend 2000

# Auto-trade without confirmation prompts
uv run python trade.py --yes auto --min-edge 10 --count 5
```

### Safety guardrails

Global flags (before the subcommand):
- `--dry-run` — print trades without executing
- `--yes` / `-y` — skip confirmation prompts
- `--max-contracts N` — max contracts per order (default: 20)
- `--max-spend N` — max total spend in cents across all orders in a run (default: 5000 = $50)

Auto-specific flags:
- `--refresh` — re-fetch Kalshi and Underdog data before scanning for edges

### Generated files

- `edges.csv` — Raw data for every detected edge (written on each auto run). Includes Kalshi bid/ask, Underdog payout multiplier, implied probability, and computed edge for manual verification.
- `trades_log.csv` — Append-only log of every real order placed. Used to deduplicate: subsequent auto runs skip any `(ticker, side)` already in the log. Delete a row (or the whole file) to allow re-trading.

## Key Concepts

- **Prices are in cents (1-99)** matching Kalshi's binary contract model. A yes_ask of 45 means 45 cents, implying ~45% probability.
- **Edge detection** compares Kalshi ask prices against Underdog implied probabilities. Underdog `payout_multiplier` converts to implied probability as `1 / (1 + payout_multiplier)`.
- **Threshold matching** between platforms: Kalshi "N+" (>= N) maps to Underdog "over N-0.5".
- **Series tickers** identify stat types: KXNBAPTS=Points, KXNBAREB=Rebounds, KXNBAAST=Assists, KXNBA3PT=3-Pointers Made, KXNBASTL=Steals, KXNBABLK=Blocks.
- **Trade deduplication** — Real (non-dry-run) orders are logged to `trades_log.csv`. On subsequent runs, edges matching an already-traded `(ticker, side)` are skipped automatically.

## Dependencies

- `pykalshi[dataframe]` — Kalshi API client (provides `KalshiClient`, `Action`, `Side`, `OrderType` enums, `to_dataframe`). Trading methods live on `client.portfolio` (e.g., `client.portfolio.place_order(...)`).
- `python-dotenv` — loads `.env` credentials
- `pandas` — data manipulation (installed via pykalshi's dataframe extra)
- `requests` — HTTP client for Underdog API (transitive dependency)
