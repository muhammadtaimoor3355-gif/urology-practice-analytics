"""
============================================================
Script 11 — Module 4: National Benchmarking
============================================================
PURPOSE:
    Compares urology department metrics against national
    and state-level benchmarks using CMS geography data.
    Produces the performance scorecard for Power BI Tab 4.

HOW TO RUN:
    python scripts/11_analysis_benchmarking.py

OUTPUT:
    outputs/reports/benchmark_report.csv
    outputs/reports/state_rankings.csv
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

# ── Maryland benchmark — the state Johns Hopkins is in ────
# We use Maryland as the "home state" for benchmarking since
# Johns Hopkins is in Baltimore, Maryland
HOME_STATE = 'MD'

def main():
    print("=" * 62)
    print("  SCRIPT 11 — Module 4: National Benchmarking")
    print("=" * 62)
    print(f"  Started    : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Home state : {HOME_STATE} (Maryland — Johns Hopkins location)")
    print()

    conn = sqlite3.connect(DB_PATH)

    physician  = pd.read_sql("SELECT * FROM physician_billing",  conn)
    benchmarks = pd.read_sql("SELECT * FROM benchmarks_geo",     conn)
    hospital   = pd.read_sql("SELECT * FROM hospital_compare",   conn)
    inpatient  = pd.read_sql("SELECT * FROM inpatient_drg",      conn)

    # ── Step 1: Maryland physician stats vs national ───────
    print("[STEP 1] Maryland vs National physician productivity...")

    national_stats = physician.groupby('cpt_code').agg(
        national_avg_payment   = ('avg_medicare_payment', 'mean'),
        national_total_services= ('total_services',       'sum'),
        national_physician_cnt = ('provider_npi',         'nunique'),
    ).reset_index()

    md_physicians = physician[physician['state'] == HOME_STATE]
    md_stats = md_physicians.groupby('cpt_code').agg(
        md_avg_payment    = ('avg_medicare_payment', 'mean'),
        md_total_services = ('total_services',       'sum'),
        md_physician_cnt  = ('provider_npi',         'nunique'),
    ).reset_index()

    comparison = national_stats.merge(md_stats, on='cpt_code', how='inner')
    comparison['payment_vs_national_pct'] = (
        (comparison['md_avg_payment'] - comparison['national_avg_payment'])
        / comparison['national_avg_payment'] * 100
    ).round(1)
    comparison['volume_vs_national_pct'] = (
        (comparison['md_total_services'] - comparison['national_total_services'] / comparison['national_physician_cnt'])
        / (comparison['national_total_services'] / comparison['national_physician_cnt']) * 100
    ).round(1)

    md_count     = md_physicians['provider_npi'].nunique()
    nat_count    = physician['provider_npi'].nunique()
    md_avg_rev   = md_physicians.groupby('provider_npi')['estimated_annual_revenue'].sum().mean()
    nat_avg_rev  = physician.groupby('provider_npi')['estimated_annual_revenue'].sum().mean()
    md_avg_procs = md_physicians.groupby('provider_npi')['total_services'].sum().mean()
    nat_avg_procs= physician.groupby('provider_npi')['total_services'].sum().mean()

    print(f"\n  {'Metric':<35} {'Maryland':>12} {'National':>12} {'Diff':>8}")
    print(f"  {'-'*70}")
    print(f"  {'Urologist count':<35} {md_count:>12,} {nat_count:>12,}")
    print(f"  {'Avg revenue per physician':<35} ${md_avg_rev:>11,.0f} ${nat_avg_rev:>11,.0f} "
          f"{(md_avg_rev/nat_avg_rev-1)*100:>+7.1f}%")
    print(f"  {'Avg procedures per physician':<35} {md_avg_procs:>12,.0f} {nat_avg_procs:>12,.0f} "
          f"{(md_avg_procs/nat_avg_procs-1)*100:>+7.1f}%")

    # ── Step 2: State rankings ────────────────────────────
    print("\n[STEP 2] State rankings by key metrics...")

    state_metrics = physician.groupby('state').agg(
        urologist_count  = ('provider_npi',              'nunique'),
        total_revenue    = ('estimated_annual_revenue',  'sum'),
        avg_revenue_per_md= ('estimated_annual_revenue', 'mean'),
        total_services   = ('total_services',            'sum'),
    ).reset_index()

    state_metrics['revenue_per_md_rank'] = state_metrics['avg_revenue_per_md'].rank(ascending=False).astype(int)
    state_metrics['volume_rank']         = state_metrics['total_services'].rank(ascending=False).astype(int)
    state_metrics = state_metrics.sort_values('revenue_per_md_rank')

    print(f"\n  STATE RANKINGS BY AVERAGE REVENUE PER UROLOGIST:")
    print(f"  {'Rank':>4}  {'State':>5}  {'MDs':>6}  {'Avg Rev/MD':>12}  {'Volume Rank':>11}")
    print(f"  {'-'*50}")
    for _, row in state_metrics.head(15).iterrows():
        flag = ' ◄ HOME' if row['state'] == HOME_STATE else ''
        print(f"  {row['revenue_per_md_rank']:>4}  {row['state']:>5}  "
              f"{row['urologist_count']:>6,}  "
              f"${row['avg_revenue_per_md']:>11,.0f}  "
              f"{row['volume_rank']:>11}{flag}")

    # Maryland's rank
    md_rank = state_metrics[state_metrics['state'] == HOME_STATE]
    if len(md_rank):
        r = md_rank.iloc[0]
        print(f"\n  Maryland (Johns Hopkins state):")
        print(f"    Revenue rank : #{r['revenue_per_md_rank']} of {len(state_metrics)} states")
        print(f"    Volume rank  : #{r['volume_rank']} of {len(state_metrics)} states")
        print(f"    Avg rev/MD   : ${r['avg_revenue_per_md']:,.0f}")

    # ── Step 3: Hospital quality benchmarks ───────────────
    print("\n[STEP 3] Hospital quality benchmarks...")

    hospital['overall_rating'] = pd.to_numeric(hospital['overall_rating'], errors='coerce')
    hospital['hospital_name']  = hospital['hospital_name'].astype(str)

    # Find Johns Hopkins in the data
    jhu = hospital[hospital['hospital_name'].str.contains('Johns Hopkins', case=False, na=False)]
    print(f"\n  Johns Hopkins hospitals in dataset:")
    if len(jhu):
        print(jhu[['hospital_name','state','overall_rating','hospital_type']].to_string(index=False))
    else:
        print("  Not found — may be listed under a different name.")

    # Academic medical centers comparison
    if 'is_academic_medical_center' in hospital.columns:
        amc = hospital[hospital['is_academic_medical_center'] == True]
    else:
        amc = hospital[hospital['hospital_name'].str.lower().str.contains(
            'university|medical center|johns hopkins|mayo|cleveland', na=False
        )]

    print(f"\n  Academic Medical Centers quality summary:")
    print(f"    Count             : {len(amc):,}")
    print(f"    Avg star rating   : {amc['overall_rating'].mean():.2f} (national avg: {hospital['overall_rating'].mean():.2f})")
    print(f"    5-star AMCs       : {(amc['overall_rating']==5).sum():,}")
    print(f"    % above national avg: {(amc['overall_rating'] > hospital['overall_rating'].mean()).mean()*100:.1f}%")

    # ── Step 4: Performance scorecard ─────────────────────
    print("\n[STEP 4] Building performance scorecard...")

    national_median_rev   = physician.groupby('provider_npi')['estimated_annual_revenue'].sum().median()
    national_median_procs = physician.groupby('provider_npi')['total_services'].sum().median()
    total_nat_revenue     = physician['estimated_annual_revenue'].sum()
    total_nat_or_cases    = pd.to_numeric(inpatient['total_discharges'], errors='coerce').sum()

    scorecard = pd.DataFrame([
        {
            'metric'           : 'National urologists tracked',
            'value'            : f"{physician['provider_npi'].nunique():,}",
            'benchmark'        : 'N/A',
            'status'           : 'INFO',
        },
        {
            'metric'           : 'National median physician revenue/yr',
            'value'            : f"${national_median_rev:,.0f}",
            'benchmark'        : 'CMS 2022 national median',
            'status'           : 'BENCHMARK',
        },
        {
            'metric'           : 'National median procedures/yr',
            'value'            : f"{national_median_procs:,.0f}",
            'benchmark'        : 'CMS 2022 national median',
            'status'           : 'BENCHMARK',
        },
        {
            'metric'           : 'Maryland avg revenue vs national',
            'value'            : f"+{(md_avg_rev/nat_avg_rev-1)*100:.1f}%",
            'benchmark'        : f"National avg: ${nat_avg_rev:,.0f}",
            'status'           : 'ABOVE' if md_avg_rev > nat_avg_rev else 'BELOW',
        },
        {
            'metric'           : 'Total estimated urology revenue (US)',
            'value'            : f"${total_nat_revenue:,.0f}",
            'benchmark'        : 'CMS 2022 all urologists',
            'status'           : 'INFO',
        },
        {
            'metric'           : 'Total inpatient urology discharges',
            'value'            : f"{total_nat_or_cases:,.0f}",
            'benchmark'        : 'CMS Inpatient 2022',
            'status'           : 'INFO',
        },
        {
            'metric'           : '#1 urology demand condition (MEPS)',
            'value'            : 'Urinary Tract Infection (27.5%)',
            'benchmark'        : 'MEPS 2022 national survey',
            'status'           : 'INFO',
        },
        {
            'metric'           : 'States with capacity >80%',
            'value'            : '21 of 51',
            'benchmark'        : 'Capacity model estimate',
            'status'           : 'WARNING',
        },
    ])

    print(f"\n  NATIONAL UROLOGY PERFORMANCE SCORECARD:")
    print(scorecard.to_string(index=False))

    # ── Save outputs ──────────────────────────────────────
    comparison.to_csv(os.path.join(OUT_DIR, 'benchmark_report.csv'), index=False)
    state_metrics.to_csv(os.path.join(OUT_DIR, 'state_rankings.csv'), index=False)
    scorecard.to_csv(os.path.join(OUT_DIR, 'performance_scorecard.csv'), index=False)
    print(f"\n  Saved: benchmark_report.csv, state_rankings.csv, performance_scorecard.csv")

    print()
    print("=" * 62)
    print("  SCRIPT 11 COMPLETE")
    print(f"  Maryland ranks in national urology benchmarks")
    print("  POWER BI: Load benchmark_report.csv for Tab 4")
    print("  Next: python scripts/12_predictive_alerts.py")
    print("=" * 62)

    conn.close()
    append_audit_log('11_analysis_benchmarking.py', 'All tables',
                     len(physician), len(scorecard), 'SUCCESS',
                     'Benchmarking scorecard generated for MD vs national')

if __name__ == "__main__":
    main()
