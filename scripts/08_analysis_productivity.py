"""
============================================================
Script 08 — Module 1: Physician Productivity Analysis
============================================================
PURPOSE:
    Analyzes physician productivity from the CMS billing data.
    Produces the CSV that feeds Power BI Tab 1.

    WHAT IS "PRODUCTIVITY" IN UROLOGY?
    In hospital operations, physician productivity is measured
    in RVUs — Relative Value Units. Every CPT procedure code
    has an assigned RVU value set by Medicare. A cystoscopy
    (52000) is worth more RVUs than a simple office visit
    (99213). Summing all RVUs billed by a physician tells
    you how much clinical work they did.

    However — the CMS public dataset does NOT include RVU
    values directly. We APPROXIMATE RVUs using the Medicare
    allowed amount as a proxy (higher payment = higher RVU).
    This is standard practice in health services research
    when the actual RVU file is not available.

    WHAT THIS SCRIPT CALCULATES:
    For each physician (by NPI):
      - Total procedures performed (sum of total_services)
      - Total Medicare revenue generated
      - Estimated RVU proxy (total_services × avg_allowed_amount)
      - Unique CPT codes billed (procedure variety)
      - Primary state of practice
      - Top procedure by volume

    BENCHMARKING:
    Each physician's productivity is compared against:
      - National median for all urologists
      - Top 25th percentile (above average)
      - Top 10th percentile (high performer)
    This creates the "above/at/below benchmark" flags
    used in the Power BI dashboard.

HOW TO RUN:
    python scripts/08_analysis_productivity.py

OUTPUT:
    outputs/reports/productivity_report.csv
============================================================
"""

import os
import sys
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime

# ── Project root ──────────────────────────────────────────
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.join(PROJECT_ROOT, 'scripts'))

try:
    from _00_setup_environment import append_audit_log
except ImportError:
    def append_audit_log(*a, **k): pass

DB_PATH  = os.path.join(PROJECT_ROOT, 'data', 'processed', 'master_database.sqlite')
OUT_DIR  = os.path.join(PROJECT_ROOT, 'outputs', 'reports')
OUT_FILE = os.path.join(OUT_DIR, 'productivity_report.csv')
os.makedirs(OUT_DIR, exist_ok=True)

# ── Main ──────────────────────────────────────────────────

def main():
    print("=" * 62)
    print("  SCRIPT 08 — Module 1: Physician Productivity Analysis")
    print("=" * 62)
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    conn = sqlite3.connect(DB_PATH)

    # ── Step 1: Load physician billing data ───────────────
    print("[STEP 1] Loading physician billing data from database...")
    df = pd.read_sql("SELECT * FROM physician_billing", conn)
    print(f"  Rows loaded: {len(df):,}")

    # ── Step 2: Aggregate to physician level ──────────────
    print("\n[STEP 2] Aggregating to physician level (one row per NPI)...")

    # For each physician, compute productivity metrics
    agg = df.groupby('provider_npi').agg(
        last_name             = ('last_name',              'first'),
        first_name            = ('first_name',             'first'),
        credentials           = ('credentials',            'first'),
        state                 = ('state',                  'first'),
        city                  = ('city',                   'first'),
        total_procedures      = ('total_services',         'sum'),
        total_beneficiaries   = ('total_beneficiaries',    'sum'),
        total_revenue         = ('estimated_annual_revenue','sum'),
        unique_cpt_codes      = ('cpt_code',               'nunique'),
        avg_payment_per_proc  = ('avg_medicare_payment',   'mean'),
    ).reset_index()

    # RVU proxy = sum of (total_services × avg_allowed_amount)
    # avg_allowed_amount is our RVU stand-in
    df['rvu_proxy'] = df['total_services'] * df['avg_allowed_amount']
    rvu_by_npi = df.groupby('provider_npi')['rvu_proxy'].sum().reset_index()
    rvu_by_npi.columns = ['provider_npi', 'total_rvu_proxy']
    agg = agg.merge(rvu_by_npi, on='provider_npi', how='left')

    # Top CPT code by volume for each physician
    top_cpt = (df.sort_values('total_services', ascending=False)
                 .groupby('provider_npi')[['cpt_code', 'cpt_description', 'total_services']]
                 .first()
                 .reset_index()
                 .rename(columns={
                     'cpt_code'       : 'top_cpt_code',
                     'cpt_description': 'top_cpt_description',
                     'total_services' : 'top_cpt_volume',
                 }))
    agg = agg.merge(top_cpt, on='provider_npi', how='left')

    print(f"  Physicians after aggregation: {len(agg):,}")

    # ── Step 3: National benchmarks ───────────────────────
    print("\n[STEP 3] Calculating national benchmark percentiles...")

    p10  = agg['total_rvu_proxy'].quantile(0.10)
    p25  = agg['total_rvu_proxy'].quantile(0.25)
    p50  = agg['total_rvu_proxy'].quantile(0.50)
    p75  = agg['total_rvu_proxy'].quantile(0.75)
    p90  = agg['total_rvu_proxy'].quantile(0.90)

    print(f"  RVU Proxy Benchmarks (all US urologists):")
    print(f"    10th percentile : {p10:>12,.0f}")
    print(f"    25th percentile : {p25:>12,.0f}")
    print(f"    50th (median)   : {p50:>12,.0f}  ← national median")
    print(f"    75th percentile : {p75:>12,.0f}")
    print(f"    90th percentile : {p90:>12,.0f}  ← high performer")

    # Add benchmark columns
    agg['national_median_rvu']   = p50
    agg['national_p75_rvu']      = p75
    agg['national_p90_rvu']      = p90
    agg['pct_of_median']         = (agg['total_rvu_proxy'] / p50 * 100).round(1)

    def performance_tier(rvu):
        if rvu >= p90: return 'TOP PERFORMER (>90th pct)'
        if rvu >= p75: return 'ABOVE AVERAGE (75-90th pct)'
        if rvu >= p25: return 'AT BENCHMARK (25-75th pct)'
        return 'BELOW BENCHMARK (<25th pct)'

    agg['performance_tier'] = agg['total_rvu_proxy'].apply(performance_tier)

    # Productivity trend flag (vs median)
    agg['vs_national_median'] = (agg['total_rvu_proxy'] - p50).round(0)
    agg['above_median']       = agg['total_rvu_proxy'] >= p50

    # ── Step 4: State-level summary ───────────────────────
    print("\n[STEP 4] State-level productivity summary...")
    state_summary = agg.groupby('state').agg(
        physician_count     = ('provider_npi',    'count'),
        avg_rvu_proxy       = ('total_rvu_proxy', 'mean'),
        avg_procedures      = ('total_procedures','mean'),
        avg_revenue         = ('total_revenue',   'mean'),
        median_rvu          = ('total_rvu_proxy', 'median'),
    ).round(2).reset_index()
    state_summary = state_summary.sort_values('physician_count', ascending=False)

    print(f"  Top 10 states by urologist count:")
    print(state_summary[['state','physician_count','avg_rvu_proxy','avg_revenue']].head(10).to_string(index=False))

    # Save state summary
    state_path = os.path.join(OUT_DIR, 'productivity_by_state.csv')
    state_summary.to_csv(state_path, index=False)
    print(f"  Saved state summary: {state_path}")

    # ── Step 5: Top / bottom performers ───────────────────
    print("\n[STEP 5] Top and bottom performers...")
    top10    = agg.nlargest(10, 'total_rvu_proxy')[
        ['provider_npi','last_name','first_name','state',
         'total_procedures','total_revenue','total_rvu_proxy','performance_tier']
    ]
    bottom10 = agg.nsmallest(10, 'total_rvu_proxy')[
        ['provider_npi','last_name','first_name','state',
         'total_procedures','total_revenue','total_rvu_proxy','performance_tier']
    ]

    print(f"\n  TOP 10 PRODUCERS (by RVU proxy):")
    print(top10[['last_name','first_name','state','total_procedures','total_revenue']].to_string(index=False))

    print(f"\n  BOTTOM 10 PRODUCERS (by RVU proxy):")
    print(bottom10[['last_name','first_name','state','total_procedures','total_revenue']].to_string(index=False))

    # ── Step 6: Performance tier distribution ─────────────
    print("\n[STEP 6] Performance tier distribution:")
    tier_counts = agg['performance_tier'].value_counts()
    for tier, count in tier_counts.items():
        pct = count / len(agg) * 100
        bar = '█' * int(pct / 2)
        print(f"  {tier:<35} {count:5,}  ({pct:.1f}%)  {bar}")

    # ── Step 7: Save final report ──────────────────────────
    print(f"\n[STEP 7] Saving productivity report...")

    # Round numeric columns for cleaner CSV
    for col in ['total_revenue', 'total_rvu_proxy', 'avg_payment_per_proc',
                'national_median_rvu', 'national_p75_rvu', 'national_p90_rvu']:
        if col in agg.columns:
            agg[col] = agg[col].round(2)

    agg.to_csv(OUT_FILE, index=False)
    size_kb = os.path.getsize(OUT_FILE) / 1024
    print(f"  Saved: {OUT_FILE}  ({size_kb:.0f} KB)")
    print(f"  Rows : {len(agg):,} (one row per urologist)")

    # ── Summary stats for interview ────────────────────────
    print()
    print("=" * 62)
    print("  KEY FINDINGS — USE THESE IN YOUR INTERVIEW")
    print("=" * 62)
    print(f"  Total unique urologists analyzed  : {len(agg):,}")
    print(f"  National median procedures/yr     : {agg['total_procedures'].median():,.0f}")
    print(f"  National median revenue/yr        : ${agg['total_revenue'].median():,.0f}")
    print(f"  Top performer revenue             : ${agg['total_revenue'].max():,.0f}")
    print(f"  Above-median performers           : {agg['above_median'].sum():,} ({agg['above_median'].mean()*100:.1f}%)")
    top_state = state_summary.iloc[0]
    print(f"  Most urologists in one state      : {top_state['state']} ({top_state['physician_count']:,})")
    print()
    print("  POWER BI: Load productivity_report.csv for Tab 1")
    print("  Next: python scripts/09_analysis_billing.py")
    print("=" * 62)

    conn.close()
    append_audit_log('08_analysis_productivity.py', 'physician_billing table',
                     len(df), len(agg), 'SUCCESS',
                     f"{len(agg):,} physicians, median revenue ${agg['total_revenue'].median():,.0f}")


if __name__ == "__main__":
    main()
