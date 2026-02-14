"""Rich terminal dashboard for trade tracking and ROI.

Pulls fills, settlements, and market titles directly from the Kalshi API
instead of relying on locally-computed P&L.
"""

import argparse
import sys

from rich.console import Console
from rich.panel import Panel
from rich.table import Table, box
from rich.text import Text

from src.common import SERIES_TO_STAT, parse_kalshi_title
from src.main import get_client

console = Console()

BLOCK_FULL = "█"
BLOCK_UPPER = "▀"
BLOCK_LOWER = "▄"
CHART_HEIGHT = 12


def _stat_from_ticker(ticker):
    """Extract stat name from a Kalshi ticker's series prefix."""
    series = ticker.split("-")[0]
    return SERIES_TO_STAT.get(series, "Unknown")


def _fee_cents(fee_cost):
    """Convert fee_cost (dollar string like '0.3200' or None) to integer cents."""
    if not fee_cost:
        return 0
    return round(float(fee_cost) * 100)


def fetch_data(client):
    """Fetch fills, settlements, and market titles from Kalshi.

    Returns (fills, settlements_by_ticker, market_titles).
    """
    console.print("[dim]Fetching fills from Kalshi...[/dim]")
    fills = client.portfolio.get_fills(fetch_all=True)

    # Filter to NBA player prop tickers only
    fills = [f for f in fills if f.ticker.split("-")[0] in SERIES_TO_STAT]

    console.print("[dim]Fetching settlements from Kalshi...[/dim]")
    settlements = client.portfolio.get_settlements(fetch_all=True)
    settlements_by_ticker = {}
    for s in settlements:
        if s.ticker.split("-")[0] in SERIES_TO_STAT:
            settlements_by_ticker[s.ticker] = s

    # Fetch market titles for player names
    tickers = list({f.ticker for f in fills})
    market_titles = {}
    batch_size = 100
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i : i + batch_size]
        try:
            markets = client.get_markets(tickers=batch)
            for m in markets:
                market_titles[m.ticker] = m.title
        except Exception as e:
            console.print(f"[yellow]Warning: failed to fetch market titles: {e}[/yellow]")

    console.print(f"[dim]Loaded {len(fills)} fills, {len(settlements_by_ticker)} settlements[/dim]")
    return fills, settlements_by_ticker, market_titles


def render_portfolio_summary(fills, settlements_by_ticker):
    """Render the portfolio summary panel."""
    traded_tickers = {f.ticker for f in fills}
    settled_tickers = {t for t in traded_tickers if t in settlements_by_ticker}
    pending_tickers = traded_tickers - settled_tickers

    wins = 0
    losses = 0
    net_pnl = 0
    total_fees = 0
    for ticker in settled_tickers:
        s = settlements_by_ticker[ticker]
        pnl = s.pnl
        net_pnl += pnl
        total_fees += _fee_cents(s.fee_cost)
        if pnl > 0:
            wins += 1
        elif pnl < 0:
            losses += 1
        # pnl == 0 counts as neither win nor loss

    total_spent = 0
    for f in fills:
        if f.action.value == "buy":
            price = f.yes_price if f.side.value == "yes" else f.no_price
            total_spent += price * f.count

    settled = len(settled_tickers)
    win_rate = (wins / settled * 100) if settled > 0 else 0
    roi = (net_pnl / total_spent * 100) if total_spent > 0 else 0

    grid = Table(show_header=False, box=None, padding=(0, 2))
    grid.add_column(style="bold")
    grid.add_column()
    grid.add_column(style="bold")
    grid.add_column()

    grid.add_row("Markets Traded", str(len(traded_tickers)), "Settled", str(settled))
    grid.add_row("Pending", str(len(pending_tickers)), "Win Rate", f"{win_rate:.1f}%")
    grid.add_row("Wins", f"[green]{wins}[/green]", "Losses", f"[red]{losses}[/red]")
    fee_pct = (total_fees / total_spent * 100) if total_spent > 0 else 0
    grid.add_row(
        "Total Spent",
        f"${total_spent / 100:.2f}",
        "Total Fees",
        f"${total_fees / 100:.2f} ({fee_pct:.1f}%)",
    )

    pnl_color = "green" if net_pnl >= 0 else "red"
    roi_color = "green" if roi >= 0 else "red"
    grid.add_row(
        "Net P&L",
        f"[{pnl_color}]${net_pnl / 100:+.2f}[/{pnl_color}]",
        "ROI",
        f"[{roi_color}]{roi:+.1f}%[/{roi_color}]",
    )

    console.print(Panel(grid, title="Portfolio Summary", border_style="blue"))


def render_trade_history(fills, settlements_by_ticker, market_titles):
    """Render the trade history table."""
    table = Table(title="Trade History", box=box.ROUNDED)
    table.add_column("Date", style="dim")
    table.add_column("Player")
    table.add_column("Stat")
    table.add_column("Threshold", justify="right")
    table.add_column("Side")
    table.add_column("Price", justify="right")
    table.add_column("Qty", justify="right")
    table.add_column("Cost", justify="right")
    table.add_column("Fee", justify="right")
    table.add_column("Result", justify="center")

    for f in fills:
        title = market_titles.get(f.ticker, "")
        player, threshold = parse_kalshi_title(title) if title else (None, None)
        if not player:
            player = f.ticker
            threshold = ""

        stat = _stat_from_ticker(f.ticker)
        date_str = f.created_time[:10] if f.created_time else ""
        price = f.yes_price if f.side.value == "yes" else f.no_price
        cost = price * f.count
        fee = _fee_cents(f.fee_cost)

        settlement = settlements_by_ticker.get(f.ticker)
        if settlement and settlement.market_result:
            won = settlement.market_result == f.side.value
            if won:
                result_text = Text("WIN", style="bold green")
            else:
                result_text = Text("LOSS", style="bold red")
        else:
            result_text = Text("PENDING", style="yellow")

        table.add_row(
            date_str,
            str(player),
            stat,
            str(threshold),
            f.side.value,
            f"{price}¢",
            str(f.count),
            f"${cost / 100:.2f}",
            f"${fee / 100:.2f}",
            result_text,
        )

    console.print(table)


def render_performance_by_stat(fills, settlements_by_ticker):
    """Render performance grouped by stat type."""
    stats = {}
    # Aggregate fills by ticker first, then by stat
    ticker_stats = {}
    for f in fills:
        stat = _stat_from_ticker(f.ticker)
        if f.ticker not in ticker_stats:
            ticker_stats[f.ticker] = stat
        if stat not in stats:
            stats[stat] = {"markets": set(), "wins": 0, "losses": 0, "pending": 0, "spent": 0, "pnl": 0}
        s = stats[stat]
        s["markets"].add(f.ticker)
        if f.action.value == "buy":
            price = f.yes_price if f.side.value == "yes" else f.no_price
            s["spent"] += price * f.count

    # Apply settlement P&L per ticker (once per market, not per fill)
    counted = set()
    for ticker, stat in ticker_stats.items():
        if ticker in counted:
            continue
        counted.add(ticker)
        s = stats[stat]
        settlement = settlements_by_ticker.get(ticker)
        if settlement and settlement.market_result:
            pnl = settlement.pnl
            s["pnl"] += pnl
            if pnl > 0:
                s["wins"] += 1
            elif pnl < 0:
                s["losses"] += 1
        else:
            s["pending"] += 1

    table = Table(title="Performance by Stat", box=box.ROUNDED)
    table.add_column("Stat")
    table.add_column("Markets", justify="right")
    table.add_column("W", justify="right", style="green")
    table.add_column("L", justify="right", style="red")
    table.add_column("Pending", justify="right", style="yellow")
    table.add_column("Spent", justify="right")
    table.add_column("P&L", justify="right")
    table.add_column("ROI", justify="right")

    for stat in sorted(stats):
        s = stats[stat]
        pnl_color = "green" if s["pnl"] >= 0 else "red"
        roi = (s["pnl"] / s["spent"] * 100) if s["spent"] > 0 else 0
        roi_color = "green" if roi >= 0 else "red"
        table.add_row(
            stat,
            str(len(s["markets"])),
            str(s["wins"]),
            str(s["losses"]),
            str(s["pending"]),
            f"${s['spent'] / 100:.2f}",
            f"[{pnl_color}]${s['pnl'] / 100:+.2f}[/{pnl_color}]",
            f"[{roi_color}]{roi:+.1f}%[/{roi_color}]",
        )

    console.print(table)


def render_cumulative_chart(settlements_by_ticker):
    """Render a multi-row cumulative P&L bar chart."""
    settled = [
        s for s in settlements_by_ticker.values()
        if s.market_result
    ]
    settled.sort(key=lambda s: s.settled_time or "")

    if not settled:
        console.print(Panel("[dim]No settled markets yet[/dim]", title="Cumulative P&L", border_style="blue"))
        return

    # Build cumulative series
    cumulative = []
    running = 0
    for s in settled:
        running += s.pnl
        cumulative.append(running)

    # Determine chart bounds (always include zero line)
    max_val = max(max(cumulative), 0)
    min_val = min(min(cumulative), 0)
    span = max_val - min_val if max_val != min_val else 1

    # Scale values to chart rows (0 = bottom, CHART_HEIGHT-1 = top)
    zero_row = int((0 - min_val) / span * (CHART_HEIGHT - 1))

    def val_to_row(v):
        return int((v - min_val) / span * (CHART_HEIGHT - 1))

    # Y-axis labels: top, zero, bottom
    label_width = max(len(f"${max_val / 100:+.2f}"), len(f"${min_val / 100:+.2f}")) + 1

    # Build chart rows top-down
    lines = []
    for row_idx in range(CHART_HEIGHT - 1, -1, -1):
        # Y-axis label
        if row_idx == CHART_HEIGHT - 1:
            label = f"${max_val / 100:+.2f}"
        elif row_idx == 0:
            label = f"${min_val / 100:+.2f}"
        elif row_idx == zero_row:
            label = "$0.00"
        else:
            label = ""
        label = label.rjust(label_width)

        row_text = Text(f"{label} │", style="dim")
        for v in cumulative:
            v_row = val_to_row(v)
            color = "green" if v >= 0 else "red"
            if v >= 0:
                # Bar extends from zero_row up to v_row
                if zero_row <= row_idx <= v_row:
                    row_text.append(BLOCK_FULL, style=color)
                else:
                    row_text.append(" ")
            else:
                # Bar extends from v_row up to zero_row
                if v_row <= row_idx <= zero_row:
                    row_text.append(BLOCK_FULL, style=color)
                else:
                    row_text.append(" ")
        lines.append(row_text)

    # X-axis line
    axis = Text(" " * label_width + " └" + "─" * len(cumulative), style="dim")
    lines.append(axis)

    # Summary line
    final = cumulative[-1]
    final_color = "green" if final >= 0 else "red"
    summary = Text(" " * label_width + "  ")
    summary.append(f"{len(settled)} settled markets", style="dim")
    summary.append("  →  ", style="dim")
    summary.append(f"${final / 100:+.2f}", style=f"bold {final_color}")
    lines.append(summary)

    content = Text("\n").join(lines)
    console.print(Panel(content, title="Cumulative P&L", border_style="blue"))


def main():
    parser = argparse.ArgumentParser(description="Trade performance dashboard")
    parser.add_argument("--no-fetch", action="store_true", help="Skip Kalshi API calls (show empty dashboard)")
    args = parser.parse_args()

    if args.no_fetch:
        console.print("[yellow]--no-fetch specified. Nothing to display without API access.[/yellow]")
        sys.exit(0)

    client = get_client()
    fills, settlements_by_ticker, market_titles = fetch_data(client)

    if not fills:
        console.print("[red]No NBA player prop fills found on your account.[/red]")
        sys.exit(1)

    console.print()
    render_portfolio_summary(fills, settlements_by_ticker)
    console.print()
    render_trade_history(fills, settlements_by_ticker, market_titles)
    console.print()
    render_performance_by_stat(fills, settlements_by_ticker)
    console.print()
    render_cumulative_chart(settlements_by_ticker)
    console.print()


if __name__ == "__main__":
    main()
