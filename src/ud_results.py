"""Analyze ROI and win rate from Underdog Fantasy entry slip results.

Loads all underdog_results*.json files from data/underdog_results/, deduplicates
by slip ID, and prints a summary + per-day breakdown for the specified time window.

Usage:
    uv run ud-results                  # last 2 weeks (default)
    uv run ud-results --weeks 4        # last 4 weeks
    uv run ud-results --all            # all settled slips
    uv run ud-results --by-stat        # show pick hit rates broken down by stat type
    uv run ud-results --save           # also write results to data/ud_results.csv
"""

import argparse
import json
import os
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from glob import glob

from src.common import DATA_DIR

RESULTS_CSV = os.path.join(DATA_DIR, "ud_results.csv")


def load_data() -> tuple[list[dict], dict[str, str]]:
    """Load entry slips and build option_id -> stat name lookup from all JSON files.

    Returns:
        slips:      deduplicated list of entry_slip dicts
        stat_map:   {option_id: display_stat} e.g. "Points", "Rebounds"
    """
    results_dir = os.path.join(DATA_DIR, "underdog_results")
    paths = sorted(glob(os.path.join(results_dir, "*.json")))
    if not paths:
        raise FileNotFoundError(f"No JSON files found in {results_dir}/")

    all_slips: list[dict] = []
    stat_map: dict[str, str] = {}

    for path in paths:
        with open(path) as f:
            data = json.load(f)["data"]

        all_slips.extend(data["entry_slips"])

        # Build lookup: over_under_line_id -> over_under display stat
        ou_by_id = {ou["id"]: ou for ou in data.get("over_unders", [])}
        line_to_ou: dict[str, str] = {
            line["id"]: line["over_under_id"]
            for line in data.get("over_under_lines", [])
        }

        for opt in data.get("over_under_options", []):
            ou_id = line_to_ou.get(opt["over_under_line_id"])
            ou = ou_by_id.get(ou_id) if ou_id else None
            if ou:
                app_stat = ou.get("appearance_stat") or {}
                stat = app_stat.get("display_stat") or ou.get("grid_display_title")
                if stat:
                    stat_map[opt["id"]] = stat

    # Deduplicate slips by ID
    seen: set[str] = set()
    unique: list[dict] = []
    for slip in all_slips:
        if slip["id"] not in seen:
            seen.add(slip["id"])
            unique.append(slip)

    return unique, stat_map


def filter_slips(slips: list[dict], weeks: int | None) -> list[dict]:
    """Return settled slips, optionally limited to the last N weeks."""
    settled = [s for s in slips if s.get("status") == "settled"]
    if weeks is None:
        return settled
    cutoff = datetime.now(timezone.utc) - timedelta(weeks=weeks)
    return [
        s for s in settled
        if datetime.fromisoformat(s["created_at"].replace("Z", "+00:00")) >= cutoff
    ]


def local_date(slip: dict) -> str:
    dt_utc = datetime.fromisoformat(slip["created_at"].replace("Z", "+00:00"))
    return dt_utc.astimezone().strftime("%Y-%m-%d")


def analyze(slips: list[dict]) -> dict:
    """Compute slip- and pick-level stats grouped by date."""
    days: dict[str, dict] = defaultdict(lambda: {
        "fee": 0.0, "payout": 0.0,
        "sw": 0, "sl": 0, "sr": 0,
        "pw": 0, "pl": 0,
    })

    for slip in slips:
        d = days[local_date(slip)]
        d["fee"] += float(slip.get("fee", 0))
        d["payout"] += float(slip.get("payout", 0))
        result = slip.get("display_result", "")
        if result == "won":
            d["sw"] += 1
        elif result == "lost":
            d["sl"] += 1
        else:
            d["sr"] += 1

        for group in slip.get("selection_groups", []):
            for sel in group.get("selections", []):
                r = sel.get("result", "")
                if r == "won":
                    d["pw"] += 1
                elif r == "lost":
                    d["pl"] += 1

    return dict(days)


def analyze_by_stat(slips: list[dict], stat_map: dict[str, str]) -> dict:
    """Compute pick-level win/loss counts grouped by stat type."""
    stats: dict[str, dict] = defaultdict(lambda: {"pw": 0, "pl": 0})

    for slip in slips:
        for group in slip.get("selection_groups", []):
            for sel in group.get("selections", []):
                r = sel.get("result", "")
                if r not in ("won", "lost"):
                    continue
                stat = stat_map.get(sel["option_id"], "Unknown")
                if r == "won":
                    stats[stat]["pw"] += 1
                else:
                    stats[stat]["pl"] += 1

    return dict(stats)


def print_report(days: dict, weeks: int | None) -> None:
    header = "ALL TIME" if weeks is None else f"LAST {weeks} WEEK{'S' if weeks != 1 else ''}"
    print(f"\n=== UNDERDOG RESULTS — {header} ===\n")

    col = f"  {'Date':<12} {'Slips':<16} {'Slip W%':>8} {'Pick W%':>8} {'Wagered':>9} {'Payout':>9} {'Net':>9} {'ROI':>8}"
    print(col)
    print("  " + "-" * (len(col) - 2))

    totals = {"fee": 0.0, "payout": 0.0, "sw": 0, "sl": 0, "sr": 0, "pw": 0, "pl": 0}

    for date in sorted(days):
        d = days[date]
        slip_wr = d["sw"] / (d["sw"] + d["sl"]) * 100 if (d["sw"] + d["sl"]) else 0.0
        pick_wr = d["pw"] / (d["pw"] + d["pl"]) * 100 if (d["pw"] + d["pl"]) else 0.0
        net = d["payout"] - d["fee"]
        roi = net / d["fee"] * 100 if d["fee"] else 0.0
        refund_str = f"/{d['sr']}R" if d["sr"] else ""
        slip_str = f"{d['sw']}W/{d['sl']}L{refund_str}"
        print(
            f"  {date:<12} {slip_str:<16} {slip_wr:>7.1f}%  {pick_wr:>7.1f}% "
            f" ${d['fee']:>7.2f}  ${d['payout']:>7.2f}  ${net:>+7.2f}  {roi:>+7.1f}%"
        )
        for k in totals:
            totals[k] += d[k]

    print("  " + "-" * (len(col) - 2))
    tot_slip_wr = totals["sw"] / (totals["sw"] + totals["sl"]) * 100 if (totals["sw"] + totals["sl"]) else 0.0
    tot_pick_wr = totals["pw"] / (totals["pw"] + totals["pl"]) * 100 if (totals["pw"] + totals["pl"]) else 0.0
    tot_net = totals["payout"] - totals["fee"]
    tot_roi = tot_net / totals["fee"] * 100 if totals["fee"] else 0.0
    tot_slip_str = f"{totals['sw']}W/{totals['sl']}L/{totals['sr']}R"
    print(
        f"  {'TOTAL':<12} {tot_slip_str:<16} {tot_slip_wr:>7.1f}%  {tot_pick_wr:>7.1f}% "
        f" ${totals['fee']:>7.2f}  ${totals['payout']:>7.2f}  ${tot_net:>+7.2f}  {tot_roi:>+7.1f}%"
    )
    print()


def print_stat_report(stats: dict) -> None:
    print("=== PICK HIT RATE BY STAT TYPE ===\n")
    col = f"  {'Stat':<24} {'W':>5} {'L':>5} {'Total':>7} {'Hit %':>7}"
    print(col)
    print("  " + "-" * (len(col) - 2))

    rows = sorted(stats.items(), key=lambda x: -(x[1]["pw"] + x[1]["pl"]))
    for stat, d in rows:
        total = d["pw"] + d["pl"]
        pct = d["pw"] / total * 100 if total else 0.0
        print(f"  {stat:<24} {d['pw']:>5} {d['pl']:>5} {total:>7} {pct:>6.1f}%")

    print()


COMPILED_JSON = os.path.join(DATA_DIR, "underdog_results", "underdog_results_compiled.json")


def compile_results() -> None:
    """Merge all underdog_results*.json files into a single deduplicated compiled file."""
    results_dir = os.path.join(DATA_DIR, "underdog_results")
    paths = sorted(glob(os.path.join(results_dir, "*.json")))
    if not paths:
        raise FileNotFoundError(f"No JSON files found in {results_dir}/")

    combined: dict[str, dict] = {}
    for path in paths:
        with open(path) as f:
            data = json.load(f)["data"]
        for key, items in data.items():
            if key not in combined:
                combined[key] = {}
            for item in (items or []):
                combined[key][item["id"]] = item

    output = {"data": {k: list(v.values()) for k, v in combined.items()}}

    with open(COMPILED_JSON, "w") as f:
        json.dump(output, f)

    n_slips = len(output["data"].get("entry_slips", []))

    # Remove source files that aren't the compiled output
    removed = []
    for path in paths:
        if os.path.abspath(path) != os.path.abspath(COMPILED_JSON):
            os.remove(path)
            removed.append(os.path.basename(path))

    print(f"Compiled {len(paths)} file(s) → {COMPILED_JSON}")
    print(f"  {n_slips} unique entry slips")
    if removed:
        print(f"  Removed: {', '.join(removed)}")

    # Write a fresh empty file ready for next data drop
    empty_json = os.path.join(results_dir, "underdog_results.json")
    empty_keys = list(combined.keys()) or ["entry_slips", "over_unders", "over_under_lines", "over_under_options"]
    with open(empty_json, "w") as f:
        json.dump({"data": {k: [] for k in empty_keys}}, f, indent=2)
    print(f"  Created empty: {empty_json}")


def save_csv(days: dict, path: str) -> None:
    import csv

    rows = []
    for date in sorted(days):
        d = days[date]
        slip_wr = d["sw"] / (d["sw"] + d["sl"]) * 100 if (d["sw"] + d["sl"]) else None
        pick_wr = d["pw"] / (d["pw"] + d["pl"]) * 100 if (d["pw"] + d["pl"]) else None
        net = d["payout"] - d["fee"]
        roi = net / d["fee"] * 100 if d["fee"] else None
        rows.append({
            "date": date,
            "slip_wins": d["sw"],
            "slip_losses": d["sl"],
            "slip_refunds": d["sr"],
            "slip_win_pct": round(slip_wr, 1) if slip_wr is not None else "",
            "pick_wins": d["pw"],
            "pick_losses": d["pl"],
            "pick_win_pct": round(pick_wr, 1) if pick_wr is not None else "",
            "wagered": round(d["fee"], 2),
            "payout": round(d["payout"], 2),
            "net": round(net, 2),
            "roi_pct": round(roi, 1) if roi is not None else "",
        })

    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    print(f"Saved to {path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze Underdog Fantasy entry slip results.")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--weeks", type=int, default=2, metavar="N",
                       help="Analyze slips from the last N weeks (default: 2)")
    group.add_argument("--all", action="store_true",
                       help="Include all settled slips regardless of date")
    parser.add_argument("--by-stat", action="store_true",
                        help="Show pick hit rates broken down by stat type")
    parser.add_argument("--compile", action="store_true",
                        help="Merge all JSON files in data/underdog_results/ into one compiled file, then run analysis")
    parser.add_argument("--save", action="store_true",
                        help=f"Save per-day results to {RESULTS_CSV}")
    args = parser.parse_args()

    if args.compile:
        compile_results()
        print()

    weeks = None if args.all else args.weeks

    slips, stat_map = load_data()
    filtered = filter_slips(slips, weeks)

    if not filtered:
        print("No settled slips found for the specified window.")
        return

    days = analyze(filtered)
    print_report(days, weeks)

    if args.by_stat:
        stats = analyze_by_stat(filtered, stat_map)
        print_stat_report(stats)

    if args.save:
        save_csv(days, RESULTS_CSV)
