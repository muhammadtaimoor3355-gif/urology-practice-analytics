"""
============================================================
Script 09 — Module 2: Billing Pattern Analysis
============================================================
PURPOSE:
    Analyzes CPT procedure code billing patterns across all
    US urologists. Identifies top procedures, revenue drivers,
    anomalies, and compares against national benchmarks.

    WHAT IS CPT BILLING ANALYSIS?
    Every medical procedure has a CPT (Current Procedural
    Terminology) code. When a urologist performs a cystoscopy,
    they bill CPT 52000. Medicare pays a set amount for each
    code. By analyzing which codes are billed most often and
    at what rates, a Business Analyst can:
      - Identify the department's top revenue procedures
      - Spot underbilling (charging less than national avg)
      - Flag anomalies that could indicate billing errors
        or compliance risks
      - Understand the department's procedure mix

    ANALYSES IN THIS SCRIPT:
    1. Top 20 CPT codes by total volume nationally
    2. Top 20 CPT codes by total revenue nationally
    3. Average payment per procedure vs national benchmark
    4. Billing anomaly detection (procedures >2 std deviations
       from mean frequency — potential compliance flags)
    5. Underbilling identification (avg payment significantly
       below national average for same procedure)
    6. Payer mix approximation from CMS data

HOW TO RUN:
    python scripts/09_analysis_billing.py

OUTPUT:
    outputs/reports/billing_report.csv
    outputs/reports/billing_anomalies.csv
    outputs/reports/cpt_benchmarks.csv
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

def main():
    print("=" * 62)
    print("  SCRIPT 09 — Module 2: Billing Pattern Analysis")
    print("=" * 62)
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    conn = sqlite3.connect(DB_PATH)

    # ── Load data ─────────────────────────────────────────
    print("[STEP 1] Loading billing data...")
    df = pd.read_sql("SELECT * FROM physician_billing", conn)
    bench = pd.read_sql(
        "SELECT * FROM benchmarks_geo WHERE geo_level = 'National'", conn
    )
    print(f"  Physician rows : {len(df):,}")
    print(f"  Benchmark rows : {len(bench):,}")

    # ── Step 2: Top CPT codes by volume ───────────────────
    print("\n[STEP 2] Top 20 CPT codes by national volume...")
    by_volume = (df.groupby(['cpt_code', 'cpt_description'])
                   .agg(total_services      = ('total_services',         'sum'),
                        total_beneficiaries = ('total_beneficiaries',    'sum'),
                        physician_count     = ('provider_npi',           'nunique'),
                        avg_payment         = ('avg_medicare_payment',   'mean'),
                        total_revenue       = ('estimated_annual_revenue','sum'))
                   .reset_index()
                   .sort_values('total_services', ascending=False))

    top20_volume = by_volume.head(20).copy()
    top20_volume['rank_by_volume'] = range(1, 21)

    print(f"\n  TOP 20 BY VOLUME:")
    print(f"  {'Rank':>4}  {'CPT':>8}  {'Services':>12}  {'Avg Pay':>9}  Description")
    print(f"  {'-'*70}")
    for _, row in top20_volume.iterrows():
        desc = str(row['cpt_description'])[:35]
        print(f"  {row['rank_by_volume']:>4}  {row['cpt_code']:>8}  "
              f"{row['total_services']:>12,.0f}  "
              f"${row['avg_payment']:>8.2f}  {desc}")

    # ── Step 3: Top CPT codes by revenue ──────────────────
    print("\n[STEP 3] Top 20 CPT codes by total revenue...")
    top20_revenue = by_volume.sort_values('total_revenue', ascending=False).head(20).copy()
    top20_revenue['rank_by_revenue'] = range(1, 21)

    print(f"\n  TOP 20 BY REVENUE:")
    print(f"  {'Rank':>4}  {'CPT':>8}  {'Revenue':>14}  {'Avg Pay':>9}  Description")
    print(f"  {'-'*70}")
    for _, row in top20_revenue.iterrows():
        desc = str(row['cpt_description'])[:35]
        print(f"  {row['rank_by_revenue']:>4}  {row['cpt_code']:>8}  "
              f"${row['total_revenue']:>13,.0f}  "
              f"${row['avg_payment']:>8.2f}  {desc}")

    # ── Step 4: CPT benchmark comparison ──────────────────
    print("\n[STEP 4] Comparing procedure payments vs national benchmarks...")

    # Merge physician CPT averages with national benchmark
    bench_cpt = bench[['cpt_code', 'avg_medicare_payment', 'total_services']].copy()
    bench_cpt.columns = ['cpt_code', 'national_avg_payment', 'national_total_services']

    cpt_compare = by_volume.merge(bench_cpt, on='cpt_code', how='left')
    cpt_compare['payment_vs_national'] = (
        (cpt_compare['avg_payment'] - cpt_compare['national_avg_payment'])
        / cpt_compare['national_avg_payment'] * 100
    ).round(1)
    cpt_compare['payment_gap_dollars'] = (
        cpt_compare['avg_payment'] - cpt_compare['national_avg_payment']
    ).round(2)

    # ── Step 5: Anomaly detection ─────────────────────────
    print("\n[STEP 5] Billing anomaly detection (±2 std dev)...")

    # For each CPT code, look at physician-level billing frequency
    # A physician billing a code much more or less than peers is flagged
    cpt_stats = (df.groupby('cpt_code')['total_services']
                   .agg(['mean', 'std', 'count'])
                   .reset_index())
    cpt_stats.columns = ['cpt_code', 'mean_srvcs', 'std_srvcs', 'physician_count']
    cpt_stats['upper_threshold'] = cpt_stats['mean_srvcs'] + 2 * cpt_stats['std_srvcs']
    cpt_stats['lower_threshold'] = (cpt_stats['mean_srvcs'] - 2 * cpt_stats['std_srvcs']).clip(lower=0)

    # Flag physician-CPT combos where billing is anomalous
    df_flagged = df.merge(cpt_stats, on='cpt_code', how='left')
    anomalies = df_flagged[
        (df_flagged['total_services'] > df_flagged['upper_threshold']) |
        ((df_flagged['total_services'] < df_flagged['lower_threshold']) &
         (df_flagged['lower_threshold'] > 0))
    ].copy()

    anomalies['anomaly_type'] = np.where(
        anomalies['total_services'] > anomalies['upper_threshold'],
        'HIGH_VOLUME_OUTLIER',
        'LOW_VOLUME_OUTLIER'
    )
    anomalies['deviation_from_mean'] = (
        (anomalies['total_services'] - anomalies['mean_srvcs']) / anomalies['std_srvcs']
    ).round(2)

    print(f"  Anomalous physician-CPT combinations: {len(anomalies):,}")
    print(f"  HIGH_VOLUME_OUTLIER : {(anomalies['anomaly_type']=='HIGH_VOLUME_OUTLIER').sum():,}")
    print(f"  LOW_VOLUME_OUTLIER  : {(anomalies['anomaly_type']=='LOW_VOLUME_OUTLIER').sum():,}")

    # Top anomalies to investigate
    top_anomalies = anomalies.nlargest(10, 'deviation_from_mean')[
        ['provider_npi', 'last_name', 'first_name', 'state',
         'cpt_code', 'cpt_description', 'total_services',
         'mean_srvcs', 'anomaly_type', 'deviation_from_mean']
    ]
    print(f"\n  Top 10 highest-deviation anomalies:")
    print(top_anomalies[['last_name','state','cpt_code','total_services',
                          'mean_srvcs','deviation_from_mean','anomaly_type']].to_string(index=False))

    # ── Step 6: Underbilling identification ───────────────
    print("\n[STEP 6] Identifying underbilling vs national average...")

    # Procedures where the average payment received is well below national average
    underbilling = cpt_compare[
        (cpt_compare['payment_vs_national'] < -15) &  # >15% below national avg
        (cpt_compare['national_avg_payment'].notna()) &
        (cpt_compare['total_services'] > 1000)        # only high-volume codes
    ].sort_values('payment_gap_dollars').head(15)

    if len(underbilling):
        print(f"  Procedures billing >15% below national average:")
        print(underbilling[['cpt_code','cpt_description','avg_payment',
                             'national_avg_payment','payment_vs_national',
                             'payment_gap_dollars']].to_string(index=False))
    else:
        print("  No significant underbilling detected vs national average.")

    # ── Step 7: Payer mix approximation ───────────────────
    # CMS data is Medicare-only. We label it as such.
    # Place of service gives us O=Office vs F=Facility split
    print("\n[STEP 7] Place of service breakdown (billing setting)...")
    if 'place_of_service' in df.columns:
        pos_map = {'O': 'Office (outpatient)', 'F': 'Facility (hospital/ASC)'}
        pos_counts = df.groupby('place_of_service').agg(
            total_services = ('total_services', 'sum'),
            total_revenue  = ('estimated_annual_revenue', 'sum'),
        ).reset_index()
        pos_counts['place_label'] = pos_counts['place_of_service'].map(pos_map).fillna('Other')
        pos_counts['pct_volume']  = (pos_counts['total_services'] / pos_counts['total_services'].sum() * 100).round(1)
        pos_counts['pct_revenue'] = (pos_counts['total_revenue']  / pos_counts['total_revenue'].sum()  * 100).round(1)
        print(pos_counts[['place_label','total_services','pct_volume','total_revenue','pct_revenue']].to_string(index=False))

    # ── Step 8: Save all outputs ──────────────────────────
    print("\n[STEP 8] Saving billing reports...")

    billing_report = cpt_compare.sort_values('total_revenue', ascending=False)
    billing_report.to_csv(os.path.join(OUT_DIR, 'billing_report.csv'), index=False)
    print(f"  Saved: billing_report.csv  ({len(billing_report):,} CPT codes)")

    anomalies.to_csv(os.path.join(OUT_DIR, 'billing_anomalies.csv'), index=False)
    print(f"  Saved: billing_anomalies.csv  ({len(anomalies):,} flagged rows)")

    cpt_compare[['cpt_code','cpt_description','avg_payment',
                 'national_avg_payment','payment_vs_national',
                 'total_services','total_revenue']].to_csv(
        os.path.join(OUT_DIR, 'cpt_benchmarks.csv'), index=False
    )
    print(f"  Saved: cpt_benchmarks.csv")

    # ── Key findings ──────────────────────────────────────
    print()
    print("=" * 62)
    print("  KEY FINDINGS — USE THESE IN YOUR INTERVIEW")
    print("=" * 62)
    top1_vol = top20_volume.iloc[0]
    top1_rev = top20_revenue.iloc[0]
    print(f"  #1 procedure by volume : {top1_vol['cpt_code']} — {str(top1_vol['cpt_description'])[:40]}")
    print(f"     {top1_vol['total_services']:,.0f} total services nationally")
    print(f"  #1 procedure by revenue: {top1_rev['cpt_code']} — {str(top1_rev['cpt_description'])[:40]}")
    print(f"     ${top1_rev['total_revenue']:,.0f} total revenue nationally")
    print(f"  Billing anomalies found: {len(anomalies):,} physician-CPT combinations")
    print(f"  Unique CPT codes billed: {by_volume['cpt_code'].nunique():,}")
    print()
    print("  POWER BI: Load billing_report.csv for Tab 2")
    print("  Next: python scripts/10_analysis_capacity.py")
    print("=" * 62)

    conn.close()
    append_audit_log('09_analysis_billing.py', 'physician_billing + benchmarks_geo',
                     len(df), len(by_volume), 'SUCCESS',
                     f"{by_volume['cpt_code'].nunique()} CPT codes, {len(anomalies):,} anomalies")

if __name__ == "__main__":
    main()
