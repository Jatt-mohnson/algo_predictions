# Algo Predictions

NBA player prop trading system that pulls markets from Kalshi and Underdog Fantasy, detects pricing edges, and places trades on Kalshi.

## Project Structure

```
src/
  common.py          — Shared constants (stat mappings, CSV paths) and utilities
  main.py            — Fetches open NBA player prop markets from the Kalshi API
  underdog.py        — Fetches NBA player prop lines from the Underdog Fantasy public API
  draftkings.py      — Fetches NBA player prop over/under odds from DraftKings Sportsbook
  pinnacle.py        — Fetches NBA player prop over/under odds from Pinnacle Sportsbook (guest API)
  compare.py         — Joins all data sources into a unified comparison view
  trade.py           — Trading script with manual order placement and automated edge detection
  underdog_picks.py  — Finds +EV Underdog Fantasy legs using Kalshi or DraftKings as a probability oracle
data/                — Generated CSV files (gitignored)
```

## Setup

Requires Python 3.12+. Uses `uv` for dependency management.

```
uv sync
```

Kalshi API credentials go in `.env` (see `.env.example`):
- `KALSHI_API_KEY_ID` — your Kalshi API key ID
- `KALSHI_PRIVATE_KEY_PATH` — path to your private key file (e.g., `./private-key.key`)

Optional Slack integration:
- `SLACK_BOT_TOKEN` — bot token (`xoxb-...`); requires `chat:write` and `files:write` scopes
- `SLACK_CHANNEL_ID` — channel ID to post to (e.g. `C0AJPBW9GE4`); bot must be invited to the channel

Optional S3 archiving:
- `S3_BUCKET` — bucket name for `--archive` snapshots

## Usage Examples

### Fetch market data

```bash
# Pull current Kalshi NBA player prop markets
uv run kalshi

# Pull current Underdog Fantasy NBA player prop lines
uv run underdog

# Pull current DraftKings NBA player prop over/under odds
uv run draftkings

# Pull current Pinnacle NBA player prop over/under odds
uv run pinnacle
```

All scripts write CSV files to `data/` that `trade` and `ud-picks` read from. Run these first to get fresh data, or use `--refresh` to fetch inline.

### Underdog Fantasy pick finder

Finds Underdog legs where an external probability source (Kalshi or DraftKings) implies a higher win probability than the break-even required for a given entry payout. You manually enter the picks on Underdog.

```bash
# Default: Kalshi as source, 2-leg entry at 3.0x base payout
uv run ud-picks

# Use DraftKings vig-free implied probabilities instead
uv run ud-picks --source draftkings

# Average Kalshi + DraftKings probabilities
uv run ud-picks --source both

# Standard mode — only flat 1.0x multiplier lines (3.5x two-pick payout)
# Returns every line where the source implies > 53.5% probability
uv run ud-picks --standard
uv run ud-picks --standard --source draftkings

# Refresh data inline before scanning
uv run ud-picks --refresh
uv run ud-picks --source draftkings --refresh --standard

# Filter to strong signals only (5+ percentage point edge)
uv run ud-picks --min-edge 5

# 3-leg entry at 6x payout
uv run ud-picks --legs 3 --payout 6.0

# Save results to data/underdog_picks.csv
uv run ud-picks --save

# Post picks to Slack as a formatted image (requires SLACK_BOT_TOKEN + SLACK_CHANNEL_ID)
uv run ud-picks --slack
uv run ud-picks --source both --refresh --slack

# Archive source CSVs + picks to S3 (requires S3_BUCKET)
uv run ud-picks --archive

# Debug join matching between source and Underdog
uv run ud-picks --debug
```

#### How Underdog edge is calculated

Each Underdog line has a `payout_multiplier` per side (over/under can differ). The break-even probability for a leg accounts for both the entry payout and that leg's multiplier:

```
base_be       = (1 / total_payout) ^ (1 / legs)  × 100
required_prob = base_be / ud_multiplier
edge          = implied_prob - required_prob
```

A positive edge means the probability source implies this leg is more likely to hit than Underdog's payout implies.

**Standard mode** (`--standard`) restricts to lines with `payout_multiplier == 1.0`, which participate in Underdog's flat two-pick payout (default 3.5x, break-even 53.5% per leg). These are the lines without custom multiplier adjustments.

#### Output columns

- `ud_pick` — over or under
- `ud_mult` — Underdog's payout multiplier for this leg (omitted in standard mode)
- `prob` — implied probability from the source (%)
- `required_prob` — minimum probability needed to be +EV for this specific leg (%)
- `edge` — prob minus required_prob in percentage points (higher = better)
- `ticker` — Kalshi ticker for cross-reference (only available when source includes Kalshi)

### Manual trading

```bash
# Dry run — see what would be placed without executing
uv run trade --dry-run manual \
  --ticker KXNBAPTS-26FEB10SASLAL-SASVWEMBANYAMA1-35 \
  --action buy --side yes --count 1 --price 5 --type limit

# Place a real limit order (will prompt for confirmation)
uv run trade manual \
  --ticker KXNBAPTS-26FEB10SASLAL-SASVWEMBANYAMA1-35 \
  --action buy --side yes --count 1 --price 5 --type limit

# Skip confirmation prompt
uv run trade --yes manual \
  --ticker KXNBAPTS-26FEB10SASLAL-SASVWEMBANYAMA1-35 \
  --action buy --side yes --count 1 --price 5 --type limit
```

### Automated edge detection

```bash
# Scan for edges (dry run) — finds mispriced markets without trading
uv run trade --dry-run auto --min-edge 10

# Use a specific odds source (draftkings, pinnacle, or both)
uv run trade --dry-run auto --source draftkings --min-edge 10
uv run trade --dry-run auto --source pinnacle --min-edge 10

# Refresh data from both sources before scanning
uv run trade --dry-run auto --refresh --min-edge 10

# Trade detected edges with custom guardrails
uv run trade auto --min-edge 8 --count 3 --max-contracts 10 --max-spend 2000

# Auto-trade without confirmation prompts
uv run trade --yes auto --min-edge 10 --count 5
```

### Safety guardrails

Global flags (before the subcommand):
- `--dry-run` — print trades without executing
- `--yes` / `-y` — skip confirmation prompts
- `--max-contracts N` — max contracts per order (default: 20)
- `--max-spend N` — max total spend in cents across all orders in a run (default: 5000 = $50)

Auto-specific flags:
- `--source {draftkings,pinnacle,both}` — odds source for edge detection (default: both). When `both`, averages implied probabilities from both books.
- `--refresh` — re-fetch Kalshi and sportsbook data before scanning for edges

### Generated files

All generated files live in `data/`:

- `edges.csv` — Raw data for every detected edge (written on each auto run). Includes Kalshi bid/ask, DraftKings decimal odds, implied probability, and computed edge for manual verification.
- `trades_log.csv` — Append-only log of every real order placed. Used to deduplicate: subsequent auto runs skip any `(ticker, side)` already in the log. Delete a row (or the whole file) to allow re-trading.
- `underdog_picks.csv` — Written by `ud-picks --save`. All picks above the edge threshold from the most recent scan.

## Key Concepts

- **Prices are in cents (1-99)** matching Kalshi's binary contract model. A yes_ask of 45 means 45 cents, implying ~45% probability.
- **Edge detection** compares Kalshi ask prices against sportsbook implied probabilities (DraftKings, Pinnacle, or both averaged). `odds_decimal` converts to implied probability as `1 / odds_decimal`, then vig is removed by normalizing over+under.
- **Threshold matching** between platforms: Kalshi "N+" (>= N) maps to DraftKings/Underdog "over N-0.5".
- **Series tickers** identify stat types: KXNBAPTS=Points, KXNBAREB=Rebounds, KXNBAAST=Assists, KXNBA3PT=3-Pointers Made, KXNBASTL=Steals, KXNBABLK=Blocks.
- **Trade deduplication** — Real (non-dry-run) orders are logged to `data/trades_log.csv`. On subsequent runs, edges matching an already-traded `(ticker, side)` are skipped automatically.
- **Underdog multipliers** — Each Underdog line has a per-side `payout_multiplier`. Lines with multiplier == 1.0 participate in the standard flat payout (e.g. 3.5x for two picks). Lines with other multipliers adjust the break-even probability for that leg.

## Dependencies

- `pykalshi[dataframe]` — Kalshi API client (provides `KalshiClient`, `Action`, `Side`, `OrderType` enums, `to_dataframe`). Trading methods live on `client.portfolio` (e.g., `client.portfolio.place_order(...)`).
- `python-dotenv` — loads `.env` credentials
- `pandas` — data manipulation (installed via pykalshi's dataframe extra)
- `requests` — HTTP client for Underdog API (transitive dependency)
- `matplotlib` — renders picks as a PNG image for Slack (`--slack`)
- `slack-sdk` — posts image to Slack via `files_upload_v2` (`--slack`)
