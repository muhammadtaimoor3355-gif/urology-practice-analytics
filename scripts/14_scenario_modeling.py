"""
============================================================
Script 14 — Module 7: What-If Scenario Modeling
============================================================
PURPOSE:
    Models three "what-if" scenarios for operational planning.
    Administrators can see projected impact of changes before
    making costly decisions.

    Scenario A: Hire 2 additional physicians
    Scenario B: Add 10 OR slots per week
    Scenario C: Medicare reimbursement drops 5%

    Each scenario shows projected change in:
      - Annual patient volume capacity
      - Annual revenue
      - Capacity utilization %
      - Staffing requirements

HOW TO RUN:
    python scripts/14_scenario_modeling.py

OUTPUT:
    outputs/reports/scenario_report.csv
============================================================
"""

import os
import sys
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.join(PROJECT_ROOT, 'scripts'))
try:
    from _00_setup_environment import append_audit_log
except ImportError:
    def append_audit_log(*a, **k): pass

DB_PATH = os.path.join(PROJECT_ROOT, 'data', 'processed', 'master_database.sqlite')
OUT_DIR = os.path.join(PROJECT_ROOT, 'outputs', 'reports')
os.makedirs(OUT_DIR, exist_ok=True)

# ── Department baseline assumptions ──────────────────────
# Based on national median physician stats from Script 08
BASELINE = {
    'physicians'              : 8,      # typical academic urology dept
    'annual_clinic_days'      : 250,
    'patients_per_md_per_day' : 20,
    'or_days_per_week'        : 5,
    'or_cases_per_day'        : 4,
    'or_weeks_per_year'       : 48,
    'avg_revenue_per_proc'    : 89.50,  # national median from Script 08
    'avg_or_revenue_per_case' : 2695.9, # national avg from Script 10
    'medicare_rate'           : 1.00,   # 100% = current rates
}

def compute_metrics(cfg):
    """
    Compute annual operational metrics given a configuration dict.
    Returns a dict of calculated metrics.
    """
    # Ambulatory (clinic) metrics
    amb_capacity = (cfg['physicians'] * cfg['annual_clinic_days']
                    * cfg['patients_per_md_per_day'])
    amb_revenue  = amb_capacity * cfg['avg_revenue_per_proc'] * cfg['medicare_rate']

    # OR (surgical) metrics
    or_cases_annual = (cfg['or_days_per_week'] * cfg['or_cases_per_day']
                       * cfg['or_weeks_per_year'])
    or_revenue       = or_cases_annual * cfg['avg_or_revenue_per_case'] * cfg['medicare_rate']

    total_capacity = amb_capacity + or_cases_annual
    total_revenue  = amb_revenue + or_revenue

    # Utilization (compare to national median per physician)
    # National median: 1,400 procedures/yr (from Script 08)
    national_median_per_md = 1400
    current_workload       = cfg['physicians'] * national_median_per_md
    utilization_pct        = current_workload / amb_capacity * 100

    return {
        'ambulatory_capacity'   : int(amb_capacity),
        'ambulatory_revenue'    : round(amb_revenue, 0),
        'or_cases_annual'       : int(or_cases_annual),
        'or_revenue'            : round(or_revenue, 0),
        'total_annual_capacity' : int(total_capacity),
        'total_annual_revenue'  : round(total_revenue, 0),
        'capacity_utilization'  : round(utilization_pct, 1),
        'revenue_per_physician' : round(total_revenue / cfg['physicians'], 0),
    }

def main():
    print("=" * 62)
    print("  SCRIPT 14 — Module 7: What-If Scenario Modeling")
    print("=" * 62)
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    # ── Baseline ──────────────────────────────────────────
    baseline_metrics = compute_metrics(BASELINE)

    print("  BASELINE (current state):")
    print(f"    Physicians           : {BASELINE['physicians']}")
    print(f"    OR slots/week        : {BASELINE['or_days_per_week'] * BASELINE['or_cases_per_day']}")
    print(f"    Medicare rate        : {BASELINE['medicare_rate']*100:.0f}%")
    print(f"    Annual amb. capacity : {baseline_metrics['ambulatory_capacity']:,} patients")
    print(f"    Annual OR cases      : {baseline_metrics['or_cases_annual']:,}")
    print(f"    Total annual revenue : ${baseline_metrics['total_annual_revenue']:,.0f}")
    print(f"    Capacity utilization : {baseline_metrics['capacity_utilization']:.1f}%")

    # ── Define Scenarios ──────────────────────────────────
    scenarios = {
        'Scenario A — Hire 2 More Physicians': {
            **BASELINE,
            'physicians': BASELINE['physicians'] + 2,
        },
        'Scenario B — Add 10 OR Slots/Week': {
            **BASELINE,
            'or_cases_per_day': BASELINE['or_cases_per_day'] + 2,  # 2 more cases/day × 5 days
            'or_days_per_week': BASELINE['or_days_per_week'],
        },
        'Scenario C — Medicare Rate Drops 5%': {
            **BASELINE,
            'medicare_rate': 0.95,
        },
        'Scenario D — All Three Combined': {
            **BASELINE,
            'physicians'    : BASELINE['physicians'] + 2,
            'or_cases_per_day': BASELINE['or_cases_per_day'] + 2,
            'medicare_rate' : 0.95,
        },
    }

    rows = []
    print()

    for scenario_name, cfg in scenarios.items():
        metrics = compute_metrics(cfg)
        print(f"  {'─'*58}")
        print(f"  {scenario_name}")
        print(f"  {'─'*58}")

        delta_rev  = metrics['total_annual_revenue'] - baseline_metrics['total_annual_revenue']
        delta_cap  = metrics['total_annual_capacity'] - baseline_metrics['total_annual_capacity']
        delta_util = metrics['capacity_utilization'] - baseline_metrics['capacity_utilization']

        print(f"    Annual capacity change  : {delta_cap:>+12,} patients  "
              f"({delta_cap/baseline_metrics['total_annual_capacity']*100:>+.1f}%)")
        print(f"    Annual revenue change   : ${delta_rev:>+11,.0f}  "
              f"({delta_rev/baseline_metrics['total_annual_revenue']*100:>+.1f}%)")
        print(f"    Utilization change      : {delta_util:>+11.1f}%  pts")
        print(f"    Revenue per physician   : ${metrics['revenue_per_physician']:>,.0f}")
        print()

        row = {
            'scenario'               : scenario_name,
            'physicians'             : cfg['physicians'],
            'or_slots_per_week'      : cfg['or_days_per_week'] * cfg['or_cases_per_day'],
            'medicare_rate_pct'      : cfg['medicare_rate'] * 100,
            'ambulatory_capacity'    : metrics['ambulatory_capacity'],
            'or_cases_annual'        : metrics['or_cases_annual'],
            'total_annual_capacity'  : metrics['total_annual_capacity'],
            'total_annual_revenue'   : metrics['total_annual_revenue'],
            'capacity_utilization_pct': metrics['capacity_utilization'],
            'revenue_per_physician'  : metrics['revenue_per_physician'],
            'vs_baseline_revenue'    : delta_rev,
            'vs_baseline_capacity'   : delta_cap,
            'vs_baseline_util_pts'   : delta_util,
            'is_baseline'            : False,
        }
        rows.append(row)

    # Add baseline as first row
    baseline_row = {
        'scenario'               : 'BASELINE (current)',
        'physicians'             : BASELINE['physicians'],
        'or_slots_per_week'      : BASELINE['or_days_per_week'] * BASELINE['or_cases_per_day'],
        'medicare_rate_pct'      : 100.0,
        'ambulatory_capacity'    : baseline_metrics['ambulatory_capacity'],
        'or_cases_annual'        : baseline_metrics['or_cases_annual'],
        'total_annual_capacity'  : baseline_metrics['total_annual_capacity'],
        'total_annual_revenue'   : baseline_metrics['total_annual_revenue'],
        'capacity_utilization_pct': baseline_metrics['capacity_utilization'],
        'revenue_per_physician'  : baseline_metrics['revenue_per_physician'],
        'vs_baseline_revenue'    : 0,
        'vs_baseline_capacity'   : 0,
        'vs_baseline_util_pts'   : 0,
        'is_baseline'            : True,
    }

    all_rows = [baseline_row] + rows
    df = pd.DataFrame(all_rows)

    # ── Side-by-side comparison table ─────────────────────
    print(f"\n  SCENARIO COMPARISON TABLE:")
    print(f"  {'Scenario':<45} {'Rev ($)':>14}  {'Capacity':>10}  {'Util%':>6}")
    print(f"  {'─'*80}")
    for _, row in df.iterrows():
        flag = ' ◄ BASE' if row['is_baseline'] else ''
        print(f"  {row['scenario']:<45} ${row['total_annual_revenue']:>13,.0f}  "
              f"{row['total_annual_capacity']:>10,}  "
              f"{row['capacity_utilization_pct']:>5.1f}%{flag}")

    # ── Save ──────────────────────────────────────────────
    out_path = os.path.join(OUT_DIR, 'scenario_report.csv')
    df.to_csv(out_path, index=False)
    print(f"\n  Saved: {out_path}  ({len(df)} scenarios)")

    # ── Key findings ──────────────────────────────────────
    scen_a = df[df['scenario'].str.contains('Scenario A')].iloc[0]
    scen_b = df[df['scenario'].str.contains('Scenario B')].iloc[0]
    scen_c = df[df['scenario'].str.contains('Scenario C')].iloc[0]

    print()
    print("=" * 62)
    print("  KEY SCENARIO FINDINGS — USE IN YOUR INTERVIEW")
    print("=" * 62)
    print(f"  Hiring 2 MDs adds      : {scen_a['vs_baseline_capacity']:>+,} patient slots/yr")
    print(f"                           ${scen_a['vs_baseline_revenue']:>+,.0f} revenue/yr")
    print(f"  Adding 10 OR slots/wk  : {scen_b['vs_baseline_capacity']:>+,} surgical cases/yr")
    print(f"                           ${scen_b['vs_baseline_revenue']:>+,.0f} revenue/yr")
    print(f"  5% Medicare rate cut   : ${scen_c['vs_baseline_revenue']:>+,.0f} revenue/yr")
    print(f"  ROI: Each new physician: ${scen_a['revenue_per_physician']:>,.0f}/yr revenue")
    print()
    print("  POWER BI: Load scenario_report.csv for Tab 6")
    print("  Next: python scripts/15_generate_powerbi_exports.py")
    print("=" * 62)

    append_audit_log('14_scenario_modeling.py', 'Derived metrics',
                     0, len(df), 'SUCCESS',
                     f"{len(df)} scenarios modeled")

if __name__ == "__main__":
    main()
