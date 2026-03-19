"""Backtest: compare Poisson vs Negative Binomial threshold-adjustment accuracy.

Rather than matching against our personal entry slips, this backtest drives
off every DK odds line across all archived S3 snapshots and resolves actual
outcomes from the nba_stats DuckDB.  This gives ~10x more data points than
the slip-matched approach and covers the full range of stat types.

Two analyses are produced:

  1. DK calibration by stat — for every DK line where we can find the actual
     game result, how accurate is DK's vig-free implied probability?

  2. Poisson vs NB on threshold-adjusted lines — for every DK line, we
     synthetically shift the threshold ±0.5 and check actual outcomes at
     the shifted threshold.  Both Poisson and NB adjustments are evaluated
     against those outcomes, giving a direct model comparison.

Usage:
    uv run backtest
    uv run backtest --min-samples 20
    uv run backtest --save          # write rows to data/backtest_results.csv
"""

import argparse
import io
import os
from collections import defaultdict

import boto3
import duckdb
import numpy as np
import pandas as pd
from dotenv import load_dotenv

from src.common import (
    DATA_DIR,
    adjust_prob_for_threshold,
    normalize_player_name,
)

BACKTEST_CSV  = os.path.join(DATA_DIR, "backtest_results.csv")
DUCKDB_PATH   = os.path.join(DATA_DIR, "nba_stats.duckdb")

# Map DK / Underdog stat_name (lower) → DuckDB player_stats.stat_name
STAT_TO_DUCK: dict[str, str] = {
    "points":            "Points",
    "rebounds":          "Rebounds",
    "assists":           "Assists",
    "steals":            "Steals",
    "blocks":            "Blocks",
    "3-pointers made":   "FG3M",
    "pts + rebs + asts": "pts+rebs+asts",
    "points + rebounds": "pts+rebs",
    "points + assists":  "pts+asts",
    "rebounds + assists":"rebs+asts",
}

# Canonical stat key (used as grouping label in reports)
def stat_key(name: str) -> str:
    return name.lower().strip()


# ---------------------------------------------------------------------------
# Load DK snapshots from S3 (earliest per day)
# ---------------------------------------------------------------------------

def load_dk_all_lines(s3_client, bucket: str) -> pd.DataFrame:
    """Load first DK snapshot per day from S3 and return vig-free prob lines.

    Returns DataFrame with columns:
      date, player_norm, stat_norm, stat_value, over_prob, under_prob
    """
    paginator = s3_client.get_paginator("list_objects_v2")
    by_date: dict[str, list[str]] = defaultdict(list)
    for page in paginator.paginate(Bucket=bucket, Prefix="ud-picks/draftkings"):
        for obj in page.get("Contents", []):
            date = obj["Key"].split("/")[2].replace("dt=", "")
            by_date[date].append(obj["Key"])

    frames = []
    for date, keys in sorted(by_date.items()):
        earliest_key = sorted(keys)[0]
        obj = s3_client.get_object(Bucket=bucket, Key=earliest_key)
        raw = pd.read_parquet(io.BytesIO(obj["Body"].read()))

        over_df  = raw[raw["choice"] == "over" ][["full_name", "stat_name", "stat_value", "odds_decimal"]].copy()
        under_df = raw[raw["choice"] == "under"][["full_name", "stat_name", "stat_value", "odds_decimal"]].copy()
        over_df  = over_df.rename(columns={"odds_decimal": "over_odds"})
        under_df = under_df.rename(columns={"odds_decimal": "under_odds"})

        merged = over_df.merge(under_df, on=["full_name", "stat_name", "stat_value"], how="inner")
        merged = merged[
            merged["over_odds"].notna() & merged["under_odds"].notna() &
            (merged["over_odds"] > 0) & (merged["under_odds"] > 0)
        ]
        raw_over  = 1.0 / merged["over_odds"]
        raw_under = 1.0 / merged["under_odds"]
        overround = raw_over + raw_under
        merged["over_prob"]  = (raw_over  / overround).round(4)
        merged["under_prob"] = (raw_under / overround).round(4)
        merged["date"]        = date
        merged["player_norm"] = merged["full_name"].apply(normalize_player_name)
        merged["stat_norm"]   = merged["stat_name"].str.lower().str.strip()
        frames.append(merged[["date", "player_norm", "stat_norm", "stat_value", "over_prob", "under_prob"]])

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


# ---------------------------------------------------------------------------
# Load actual outcomes from DuckDB
# ---------------------------------------------------------------------------

def load_actual_outcomes(dates: list[str]) -> pd.DataFrame:
    """Return full-game stats for all relevant players across the given dates.

    Columns: player_norm, game_date (str), duck_stat, actual_value (float)
    """
    duck_stats = list(STAT_TO_DUCK.values())
    con = duckdb.connect(DUCKDB_PATH, read_only=True)

    placeholders = ", ".join(f"'{s}'" for s in duck_stats)
    date_min = min(dates)
    date_max = max(dates)

    df = con.execute(f"""
        SELECT player_name, CAST(game_date AS VARCHAR) AS game_date,
               stat_name AS duck_stat, CAST(stat_value AS DOUBLE) AS actual_value
        FROM player_stats
        WHERE period   = 'FullGame'
          AND stat_name IN ({placeholders})
          AND game_date BETWEEN '{date_min}' AND '{date_max}'
          AND stat_value IS NOT NULL
    """).df()
    con.close()

    df["player_norm"] = df["player_name"].apply(normalize_player_name)
    return df[["player_norm", "game_date", "duck_stat", "actual_value"]]


# ---------------------------------------------------------------------------
# Core backtest join
# ---------------------------------------------------------------------------

def run_backtest(dk_lines: pd.DataFrame, outcomes: pd.DataFrame) -> pd.DataFrame:
    """Join DK lines to actual outcomes and produce evaluation rows.

    For each DK line we produce:
      - One "exact" row: DK prob vs actual outcome at the stated threshold
      - Two "shifted" rows (threshold +0.5 and -0.5): Poisson and NB adjusted
        probabilities vs actual outcome at the shifted threshold

    Returns DataFrame with columns:
      date, player_norm, stat_norm, stat_value, direction,
      dk_prob, result,        ← exact match evaluation
      match_type,             ← 'exact' | 'shifted_up' | 'shifted_down'
      poisson_prob, nb_prob   ← only meaningful for shifted rows
    """
    # Build reverse map: duck_stat → stat_norm (may be many-to-one but each duck_stat is unique)
    duck_to_stat: dict[str, str] = {v: k for k, v in STAT_TO_DUCK.items()}

    # Outcomes lookup: (player_norm, game_date, duck_stat) → actual_value
    outcomes_lookup: dict[tuple, float] = {}
    for _, row in outcomes.iterrows():
        outcomes_lookup[(row["player_norm"], row["game_date"], row["duck_stat"])] = row["actual_value"]

    records = []

    for _, line in dk_lines.iterrows():
        sn      = line["stat_norm"]
        sv      = float(line["stat_value"])
        date    = line["date"]
        pn      = line["player_norm"]
        duck_stat = STAT_TO_DUCK.get(sn)
        if duck_stat is None:
            continue

        actual = outcomes_lookup.get((pn, date, duck_stat))
        if actual is None:
            continue

        # --- Exact row (both over and under) ---
        # DK stat_value is half-point (e.g. 23.5), over hits if actual > sv
        for direction, dk_prob in [("over", line["over_prob"]), ("under", line["under_prob"])]:
            if direction == "over":
                result = 1 if actual > sv else 0
            else:
                result = 1 if actual <= sv else 0  # under hits if actual ≤ sv (strict)

            records.append({
                "date":         date,
                "player_norm":  pn,
                "stat_norm":    sn,
                "stat_value":   sv,
                "direction":    direction,
                "dk_prob":      round(dk_prob * 100, 1),
                "result":       result,
                "match_type":   "exact",
                "poisson_prob": round(dk_prob * 100, 1),
                "nb_prob":      round(dk_prob * 100, 1),
            })

        # --- Shifted rows: adjust ±0.5 and evaluate at new threshold ---
        for shift, match_type in [(+0.5, "shifted_up"), (-0.5, "shifted_down")]:
            sv_new = sv + shift
            raw_over_prob = float(line["over_prob"])

            poisson_over = adjust_prob_for_threshold(
                raw_over_prob, from_line=sv, to_line=sv_new, stat=None
            )
            nb_over = adjust_prob_for_threshold(
                raw_over_prob, from_line=sv, to_line=sv_new, stat=sn
            )
            if poisson_over is None or nb_over is None:
                continue

            for direction in ("over", "under"):
                if direction == "over":
                    result = 1 if actual > sv_new else 0
                    p_prob = poisson_over
                    n_prob = nb_over
                else:
                    result = 1 if actual <= sv_new else 0
                    p_prob = 1.0 - poisson_over
                    n_prob = 1.0 - nb_over

                records.append({
                    "date":         date,
                    "player_norm":  pn,
                    "stat_norm":    sn,
                    "stat_value":   sv_new,
                    "direction":    direction,
                    "dk_prob":      None,
                    "result":       result,
                    "match_type":   match_type,
                    "poisson_prob": round(p_prob * 100, 1),
                    "nb_prob":      round(n_prob * 100, 1),
                })

    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def brier(probs: pd.Series, results: pd.Series) -> float:
    p = probs.astype(float) / 100.0
    r = results.astype(float)
    return float(((p - r) ** 2).mean())


def print_report(df: pd.DataFrame, min_samples: int) -> None:
    exact   = df[df["match_type"] == "exact"]
    shifted = df[df["match_type"].isin(["shifted_up", "shifted_down"])]

    print("\n=== BACKTEST RESULTS ===\n")
    print(f"Total evaluation rows: {len(df)}")
    print(f"  Exact (DK prob vs actual):      {len(exact)}")
    print(f"  Shifted ±0.5 (Poisson vs NB):   {len(shifted)}")

    # ── Section 1: DK calibration by stat ───────────────────────────────────
    print("\n─── 1. DK Calibration by Stat (exact lines) ───\n")
    hdr = f"  {'Stat':<26} {'n':>6} {'DK avg%':>8} {'Hit%':>7} {'Gap':>6} {'Brier':>8}"
    print(hdr)
    print("  " + "─" * (len(hdr) - 2))

    for stat, grp in sorted(exact.groupby("stat_norm"), key=lambda x: -len(x[1])):
        if len(grp) < min_samples:
            continue
        hit  = grp["result"].mean() * 100
        prob = grp["dk_prob"].mean()
        gap  = hit - prob
        bs   = brier(grp["dk_prob"], grp["result"])
        print(f"  {stat:<26} {len(grp):>6} {prob:>7.1f}% {hit:>6.1f}% {gap:>+5.1f}% {bs:>8.4f}")

    overall_hit  = exact["result"].mean() * 100
    overall_prob = exact["dk_prob"].mean()
    overall_bs   = brier(exact["dk_prob"], exact["result"])
    print("  " + "─" * (len(hdr) - 2))
    print(f"  {'OVERALL':<26} {len(exact):>6} {overall_prob:>7.1f}% {overall_hit:>6.1f}% "
          f"{overall_hit - overall_prob:>+5.1f}% {overall_bs:>8.4f}")

    # ── Section 2: Poisson vs NB on shifted thresholds ──────────────────────
    print("\n─── 2. Poisson vs NB on Threshold-Adjusted Lines ───\n")
    print("  Accuracy = % of rows where the model's predicted direction (>50%) matched the outcome.\n")
    hdr2 = (f"  {'Stat':<26} {'n':>6} {'Actual%':>8} "
            f"{'Pois Acc%':>10} {'NB Acc%':>8} {'Δ Acc':>7} "
            f"{'Pois Brier':>11} {'NB Brier':>9} {'NB wins?':>9}")
    print(hdr2)
    print("  " + "─" * (len(hdr2) - 2))

    total_pois_bs = total_nb_bs = total_n = 0.0
    total_pois_correct = total_nb_correct = 0

    for stat, grp in sorted(shifted.groupby("stat_norm"), key=lambda x: -len(x[1])):
        if len(grp) < min_samples:
            continue
        hit     = grp["result"].mean() * 100
        pois_bs = brier(grp["poisson_prob"], grp["result"])
        nb_bs   = brier(grp["nb_prob"],      grp["result"])
        delta_b = pois_bs - nb_bs

        # Accuracy: model predicts outcome correctly when its prob crosses 50%
        pois_correct = ((grp["poisson_prob"] > 50) == grp["result"].astype(bool)).sum()
        nb_correct   = ((grp["nb_prob"]      > 50) == grp["result"].astype(bool)).sum()
        pois_acc = pois_correct / len(grp) * 100
        nb_acc   = nb_correct   / len(grp) * 100
        delta_a  = nb_acc - pois_acc

        wins = "YES ✓" if delta_b > 0.0001 else ("tie" if abs(delta_b) <= 0.0001 else "no")
        n = len(grp)
        total_pois_bs      += pois_bs * n
        total_nb_bs        += nb_bs   * n
        total_n            += n
        total_pois_correct += pois_correct
        total_nb_correct   += nb_correct

        print(f"  {stat:<26} {n:>6} {hit:>7.1f}% "
              f"{pois_acc:>9.1f}% {nb_acc:>7.1f}% {delta_a:>+6.1f}% "
              f"{pois_bs:>11.4f} {nb_bs:>9.4f} {wins:>9}")

    if total_n:
        avg_pois   = total_pois_bs      / total_n
        avg_nb     = total_nb_bs        / total_n
        delta_b    = avg_pois - avg_nb
        overall_pois_acc = total_pois_correct / total_n * 100
        overall_nb_acc   = total_nb_correct   / total_n * 100
        delta_a    = overall_nb_acc - overall_pois_acc
        print("  " + "─" * (len(hdr2) - 2))
        print(f"  {'OVERALL':<26} {int(total_n):>6} {'50.0%':>8} "
              f"{overall_pois_acc:>9.1f}% {overall_nb_acc:>7.1f}% {delta_a:>+6.1f}% "
              f"{avg_pois:>11.4f} {avg_nb:>9.4f}")
        print()
        if delta_b > 0.0001:
            print(f"  NB is better calibrated (Brier Δ={delta_b:+.4f}, Accuracy Δ={delta_a:+.1f}pp)")
        elif delta_b < -0.0001:
            print(f"  Poisson is better calibrated (Brier Δ={-delta_b:+.4f}, Accuracy Δ={-delta_a:+.1f}pp)")
        else:
            print("  Models are equivalent on this dataset.")

    # ── Section 3: >54% confidence filter ───────────────────────────────────
    CONF = 54.0
    pois_conf = shifted[shifted["poisson_prob"] > CONF]
    nb_conf   = shifted[shifted["nb_prob"]      > CONF]

    print(f"\n─── 3. Accuracy on High-Confidence Picks (model prob > {CONF:.0f}%) ───\n")
    print(f"  Poisson filtered to {len(pois_conf)} picks  |  NB filtered to {len(nb_conf)} picks\n")

    hdr3 = f"  {'Stat':<26} {'Pois n':>7} {'Pois Acc%':>10} {'NB n':>6} {'NB Acc%':>9} {'Δ Acc':>7} {'NB wins?':>9}"
    print(hdr3)
    print("  " + "─" * (len(hdr3) - 2))

    all_stats = sorted(shifted["stat_norm"].unique())
    tot_pn = tot_nn = tot_pc = tot_nc = 0
    for stat in sorted(all_stats, key=lambda s: -len(shifted[shifted["stat_norm"] == s])):
        pg = pois_conf[pois_conf["stat_norm"] == stat]
        ng = nb_conf  [nb_conf  ["stat_norm"] == stat]
        if len(pg) < 5 and len(ng) < 5:
            continue
        p_acc = pg["result"].mean() * 100 if len(pg) else float("nan")
        n_acc = ng["result"].mean() * 100 if len(ng) else float("nan")
        delta_a = n_acc - p_acc if (len(pg) and len(ng)) else float("nan")
        wins = ("YES ✓" if delta_a > 0.5 else ("tie" if abs(delta_a) <= 0.5 else "no")) if not np.isnan(delta_a) else "—"
        tot_pn += len(pg); tot_nn += len(ng)
        tot_pc += pg["result"].sum(); tot_nc += ng["result"].sum()
        p_str = f"{p_acc:>9.1f}%" if len(pg) else "       —"
        n_str = f"{n_acc:>8.1f}%" if len(ng) else "      —"
        d_str = f"{delta_a:>+6.1f}%" if not np.isnan(delta_a) else "      —"
        print(f"  {stat:<26} {len(pg):>7} {p_str} {len(ng):>6} {n_str} {d_str} {wins:>9}")

    print("  " + "─" * (len(hdr3) - 2))
    opa = tot_pc / tot_pn * 100 if tot_pn else 0
    ona = tot_nc / tot_nn * 100 if tot_nn else 0
    print(f"  {'OVERALL':<26} {tot_pn:>7} {opa:>9.1f}% {tot_nn:>6} {ona:>8.1f}% {ona - opa:>+6.1f}%")
    print()

    # Disagreement breakdown by stat
    disagree = shifted[
        (shifted["poisson_prob"] > CONF) != (shifted["nb_prob"] > CONF)
    ]
    pois_only = disagree[disagree["poisson_prob"] > CONF]
    nb_only   = disagree[disagree["nb_prob"]      > CONF]

    print(f"  Picks where models disagree (one clears {CONF:.0f}%, the other doesn't):\n")
    hdr4 = f"  {'Stat':<26} {'Pois-only n':>12} {'Pois-only Acc%':>15} {'NB-only n':>10} {'NB-only Acc%':>13}"
    print(hdr4)
    print("  " + "─" * (len(hdr4) - 2))

    all_disagree_stats = sorted(
        set(pois_only["stat_norm"].unique()) | set(nb_only["stat_norm"].unique()),
        key=lambda s: -(
            len(pois_only[pois_only["stat_norm"] == s]) +
            len(nb_only[nb_only["stat_norm"] == s])
        )
    )
    tot_po_n = tot_no_n = 0
    tot_po_c = tot_no_c = 0
    for stat in all_disagree_stats:
        pg = pois_only[pois_only["stat_norm"] == stat]
        ng = nb_only  [nb_only  ["stat_norm"] == stat]
        if len(pg) == 0 and len(ng) == 0:
            continue
        p_str = f"{pg['result'].mean()*100:>14.1f}%" if len(pg) else "             —"
        n_str = f"{ng['result'].mean()*100:>12.1f}%" if len(ng) else "           —"
        print(f"  {stat:<26} {len(pg):>12} {p_str} {len(ng):>10} {n_str}")
        tot_po_n += len(pg); tot_no_n += len(ng)
        tot_po_c += pg["result"].sum(); tot_no_c += ng["result"].sum()

    print("  " + "─" * (len(hdr4) - 2))
    po_acc = tot_po_c / tot_po_n * 100 if tot_po_n else 0
    no_acc = tot_no_c / tot_no_n * 100 if tot_no_n else 0
    print(f"  {'TOTAL':<26} {tot_po_n:>12} {po_acc:>14.1f}% {tot_no_n:>10} {no_acc:>12.1f}%")
    print()


# ---------------------------------------------------------------------------
# Real UD/DK mismatch backtest
# ---------------------------------------------------------------------------

def load_ud_dk_mismatches(s3_client, bucket: str) -> pd.DataFrame:
    """Join every Underdog snapshot with its matching DK snapshot (same date+time).

    Returns one row per (player, stat) pair where UD and DK posted different
    lines, with columns:
      date, player_norm, stat_norm, ud_sv, dk_sv, sv_diff,
      over_odds, under_odds  (raw DK decimal odds for vig removal downstream)
    """
    paginator = s3_client.get_paginator("list_objects_v2")
    ud_keys: dict[str, str] = {}
    dk_keys: dict[str, str] = {}
    for page in paginator.paginate(Bucket=bucket, Prefix="ud-picks/underdog"):
        for o in page.get("Contents", []):
            parts = o["Key"].split("/")
            ud_keys[f"{parts[2]}/{parts[3]}"] = o["Key"]
    for page in paginator.paginate(Bucket=bucket, Prefix="ud-picks/draftkings"):
        for o in page.get("Contents", []):
            parts = o["Key"].split("/")
            dk_keys[f"{parts[2]}/{parts[3]}"] = o["Key"]

    matching = sorted(set(ud_keys) & set(dk_keys))
    frames = []
    for snap in matching:
        ud = pd.read_parquet(io.BytesIO(s3_client.get_object(Bucket=bucket, Key=ud_keys[snap])["Body"].read()))
        dk = pd.read_parquet(io.BytesIO(s3_client.get_object(Bucket=bucket, Key=dk_keys[snap])["Body"].read()))

        date = snap.split("/")[0].replace("dt=", "")
        ud["player_norm"] = ud["full_name"].apply(normalize_player_name)
        dk["player_norm"] = dk["full_name"].apply(normalize_player_name)
        ud["stat_norm"] = ud["stat_name"].str.lower().str.strip()
        dk["stat_norm"] = dk["stat_name"].str.lower().str.strip()

        over_dk  = dk[dk["choice"] == "over" ][["player_norm", "stat_norm", "stat_value", "odds_decimal"]].rename(columns={"odds_decimal": "over_odds",  "stat_value": "dk_sv"})
        under_dk = dk[dk["choice"] == "under"][["player_norm", "stat_norm", "stat_value", "odds_decimal"]].rename(columns={"odds_decimal": "under_odds", "stat_value": "dk_sv"})
        dk_pivot = over_dk.merge(under_dk, on=["player_norm", "stat_norm", "dk_sv"])

        over_ud = ud[ud["choice"] == "over"][["player_norm", "stat_norm", "stat_value"]].rename(columns={"stat_value": "ud_sv"}).drop_duplicates()
        joined  = over_ud.merge(dk_pivot, on=["player_norm", "stat_norm"], how="inner")
        joined["sv_diff"] = (joined["ud_sv"] - joined["dk_sv"]).round(1)
        joined["date"] = date
        frames.append(joined[joined["sv_diff"] != 0][
            ["date", "player_norm", "stat_norm", "ud_sv", "dk_sv", "sv_diff", "over_odds", "under_odds"]
        ])

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def run_real_mismatch_backtest(mismatches: pd.DataFrame, outcomes: pd.DataFrame) -> pd.DataFrame:
    """For each real UD/DK line mismatch, apply Poisson and NB to adjust the DK
    probability to match the Underdog threshold, then resolve against actual outcomes.

    Returns DataFrame with columns:
      date, player_norm, stat_norm, ud_sv, dk_sv, sv_diff, direction,
      dk_raw_prob, poisson_prob, nb_prob, result
    """
    outcomes_lookup: dict[tuple, float] = {}
    for _, row in outcomes.iterrows():
        outcomes_lookup[(row["player_norm"], row["game_date"], row["duck_stat"])] = row["actual_value"]

    records = []
    for _, row in mismatches.iterrows():
        sn        = row["stat_norm"]
        duck_stat = STAT_TO_DUCK.get(sn)
        if duck_stat is None:
            continue
        actual = outcomes_lookup.get((row["player_norm"], row["date"], duck_stat))
        if actual is None:
            continue

        # Vig-free DK probs at dk_sv
        raw_over  = 1.0 / float(row["over_odds"])
        raw_under = 1.0 / float(row["under_odds"])
        overround = raw_over + raw_under
        dk_over_prob  = raw_over  / overround
        dk_under_prob = raw_under / overround

        ud_sv = float(row["ud_sv"])
        dk_sv = float(row["dk_sv"])

        # Adjust DK over-prob from dk_sv → ud_sv
        poisson_over = adjust_prob_for_threshold(dk_over_prob, from_line=dk_sv, to_line=ud_sv, stat=None)
        nb_over      = adjust_prob_for_threshold(dk_over_prob, from_line=dk_sv, to_line=ud_sv, stat=sn)
        if poisson_over is None or nb_over is None:
            continue

        for direction in ("over", "under"):
            result = (1 if actual > ud_sv else 0) if direction == "over" else (1 if actual <= ud_sv else 0)
            dk_raw = dk_over_prob if direction == "over" else dk_under_prob
            p_prob = poisson_over  if direction == "over" else 1.0 - poisson_over
            n_prob = nb_over       if direction == "over" else 1.0 - nb_over

            records.append({
                "date":         row["date"],
                "player_norm":  row["player_norm"],
                "stat_norm":    sn,
                "ud_sv":        ud_sv,
                "dk_sv":        dk_sv,
                "sv_diff":      row["sv_diff"],
                "direction":    direction,
                "dk_raw_prob":  round(dk_raw  * 100, 1),
                "poisson_prob": round(p_prob  * 100, 1),
                "nb_prob":      round(n_prob  * 100, 1),
                "result":       result,
            })

    return pd.DataFrame(records)


def print_real_mismatch_report(df: pd.DataFrame, min_samples: int, conf: float = 54.0) -> None:
    print(f"\n─── 4. Real UD/DK Line Mismatches — Poisson vs NB ───\n")
    print(f"  {len(df)//2} unique mismatches ({len(df)} over+under rows) across "
          f"{df['date'].nunique()} dates\n")

    hdr = (f"  {'Stat':<26} {'n':>6} {'Actual%':>8} "
           f"{'Pois Acc%':>10} {'NB Acc%':>8} {'Δ Acc':>7} "
           f"{'Pois Brier':>11} {'NB Brier':>9} {'NB wins?':>9}")
    print(hdr)
    print("  " + "─" * (len(hdr) - 2))

    tot_pn = tot_nb = tot_n = 0
    tot_pb = tot_nb_b = 0.0
    for stat, grp in sorted(df.groupby("stat_norm"), key=lambda x: -len(x[1])):
        if len(grp) < min_samples:
            continue
        hit     = grp["result"].mean() * 100
        p_acc   = ((grp["poisson_prob"] > 50) == grp["result"].astype(bool)).mean() * 100
        n_acc   = ((grp["nb_prob"]      > 50) == grp["result"].astype(bool)).mean() * 100
        p_bs    = brier(grp["poisson_prob"], grp["result"])
        n_bs    = brier(grp["nb_prob"],      grp["result"])
        delta_a = n_acc - p_acc
        delta_b = p_bs  - n_bs
        wins    = "YES ✓" if delta_b > 0.0001 else ("tie" if abs(delta_b) <= 0.0001 else "no")
        n = len(grp)
        tot_pn += ((grp["poisson_prob"] > 50) == grp["result"].astype(bool)).sum()
        tot_nb += ((grp["nb_prob"]      > 50) == grp["result"].astype(bool)).sum()
        tot_n  += n
        tot_pb    += p_bs * n
        tot_nb_b  += n_bs * n
        print(f"  {stat:<26} {n:>6} {hit:>7.1f}% "
              f"{p_acc:>9.1f}% {n_acc:>7.1f}% {delta_a:>+6.1f}% "
              f"{p_bs:>11.4f} {n_bs:>9.4f} {wins:>9}")

    if tot_n:
        opa = tot_pn / tot_n * 100
        ona = tot_nb / tot_n * 100
        avg_pb  = tot_pb   / tot_n
        avg_nb  = tot_nb_b / tot_n
        print("  " + "─" * (len(hdr) - 2))
        print(f"  {'OVERALL':<26} {tot_n:>6} {'':>8} "
              f"{opa:>9.1f}% {ona:>7.1f}% {ona-opa:>+6.1f}% "
              f"{avg_pb:>11.4f} {avg_nb:>9.4f}")

    # >conf% confidence filter
    print(f"\n  High-confidence filter (prob > {conf:.0f}%):\n")
    pois_conf = df[df["poisson_prob"] > conf]
    nb_conf   = df[df["nb_prob"]      > conf]
    hdr2 = (f"  {'Stat':<26} {'Pois n':>7} {'Pois Acc%':>10} "
            f"{'NB n':>6} {'NB Acc%':>9} {'Δ Acc':>7}")
    print(hdr2)
    print("  " + "─" * (len(hdr2) - 2))

    tot_pn2 = tot_nn2 = tot_pc2 = tot_nc2 = 0
    all_stats = sorted(df["stat_norm"].unique(), key=lambda s: -len(df[df["stat_norm"] == s]))
    for stat in all_stats:
        pg = pois_conf[pois_conf["stat_norm"] == stat]
        ng = nb_conf  [nb_conf  ["stat_norm"] == stat]
        if len(pg) < 5 and len(ng) < 5:
            continue
        p_acc = pg["result"].mean() * 100 if len(pg) else float("nan")
        n_acc = ng["result"].mean() * 100 if len(ng) else float("nan")
        d     = n_acc - p_acc if (len(pg) and len(ng)) else float("nan")
        p_str = f"{p_acc:>9.1f}%" if len(pg) else "         —"
        n_str = f"{n_acc:>8.1f}%" if len(ng) else "        —"
        d_str = f"{d:>+6.1f}%" if not np.isnan(d) else "      —"
        tot_pn2 += len(pg); tot_nn2 += len(ng)
        tot_pc2 += int(pg["result"].sum()); tot_nc2 += int(ng["result"].sum())
        print(f"  {stat:<26} {len(pg):>7} {p_str} {len(ng):>6} {n_str} {d_str}")

    print("  " + "─" * (len(hdr2) - 2))
    opa2 = tot_pc2 / tot_pn2 * 100 if tot_pn2 else 0
    ona2 = tot_nc2 / tot_nn2 * 100 if tot_nn2 else 0
    print(f"  {'OVERALL':<26} {tot_pn2:>7} {opa2:>9.1f}% {tot_nn2:>6} {ona2:>8.1f}% {ona2-opa2:>+6.1f}%")

    # Disagreement rows
    disagree  = df[(df["poisson_prob"] > conf) != (df["nb_prob"] > conf)]
    pois_only = disagree[disagree["poisson_prob"] > conf]
    nb_only   = disagree[disagree["nb_prob"]      > conf]
    print(f"\n  Disagreements (one model clears {conf:.0f}%, other doesn't): {len(disagree)}")
    if len(pois_only):
        by_stat = pois_only.groupby("stat_norm")["result"].agg(wins="sum", total="count")
        parts = [f"{s}: {int(r.wins)}/{int(r.total)} ({r.wins/r.total*100:.0f}%)" for s, r in by_stat.iterrows()]
        print(f"    Poisson only — acc {pois_only['result'].mean()*100:.1f}%  |  " + "  ".join(parts))
    if len(nb_only):
        by_stat = nb_only.groupby("stat_norm")["result"].agg(wins="sum", total="count")
        parts = [f"{s}: {int(r.wins)}/{int(r.total)} ({r.wins/r.total*100:.0f}%)" for s, r in by_stat.iterrows()]
        print(f"    NB only      — acc {nb_only['result'].mean()*100:.1f}%  |  " + "  ".join(parts))
    print()

def main() -> None:
    parser = argparse.ArgumentParser(description="Backtest DK calibration + Poisson vs NB adjustment.")
    parser.add_argument("--min-samples", type=int, default=20,
                        help="Minimum rows per stat to display (default: 20)")
    parser.add_argument("--save", action="store_true",
                        help=f"Save evaluation rows to {BACKTEST_CSV}")
    args = parser.parse_args()

    load_dotenv()
    bucket = os.getenv("S3_BUCKET")
    if not bucket:
        print("S3_BUCKET env var not set.")
        return

    print("Loading DK lines from S3...")
    s3 = boto3.client("s3")
    dk_lines = load_dk_all_lines(s3, bucket)
    print(f"  {len(dk_lines)} lines across {dk_lines['date'].nunique()} dates "
          f"({dk_lines['date'].min()} – {dk_lines['date'].max()})")

    dates = sorted(dk_lines["date"].unique().tolist())
    print("Loading actual outcomes from DuckDB...")
    outcomes = load_actual_outcomes(dates)
    print(f"  {len(outcomes)} player-game-stat rows")

    print("Running synthetic shift backtest...")
    results = run_backtest(dk_lines, outcomes)
    print(f"  {len(results)} evaluation rows generated")

    print_report(results, args.min_samples)

    print("Loading real UD/DK line mismatches from S3...")
    mismatches = load_ud_dk_mismatches(s3, bucket)
    print(f"  {len(mismatches)} mismatch rows across {mismatches['date'].nunique()} dates")

    print("Running real mismatch backtest...")
    real_results = run_real_mismatch_backtest(mismatches, outcomes)
    print(f"  {len(real_results)} evaluation rows generated")

    print_real_mismatch_report(real_results, args.min_samples)

    if args.save:
        results.to_csv(BACKTEST_CSV, index=False)
        real_results.to_csv(BACKTEST_CSV.replace(".csv", "_real.csv"), index=False)
        print(f"Saved synthetic to {BACKTEST_CSV}")
        print(f"Saved real to {BACKTEST_CSV.replace('.csv', '_real.csv')}")


if __name__ == "__main__":
    main()
