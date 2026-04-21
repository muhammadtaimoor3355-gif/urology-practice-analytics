"""
============================================================
Script 10 — Module 3: Capacity & Access Analysis
============================================================
PURPOSE:
    Analyzes urology department capacity and patient access
    using MEPS condition data + CMS inpatient DRG data.

    WHAT IS CAPACITY ANALYSIS IN UROLOGY?
    A urology department has a maximum number of patients
    it can see per month based on:
      - Number of physicians
      - Available clinic slots
      - OR block time
    When demand (patient volume) approaches capacity,
    wait times increase and patients may go elsewhere.
    This analysis identifies:
      - Current demand volume by condition type
      - Inpatient procedure volumes by hospital
      - Capacity utilization rate
      - Which conditions drive the most OR utilization
      - State-level access gaps

    ANALYSES IN THIS SCRIPT:
    1. Condition demand from MEPS (what patients need)
    2. Inpatient procedure volumes from CMS (what hospitals do)
    3. Capacity utilization by state
    4. OR utilization by DRG (which procedures use OR most)
    5. Access gap analysis
    6. Average length of stay benchmarks

HOW TO RUN:
    python scripts/10_analysis_capacity.py

OUTPUT:
    outputs/reports/capacity_report.csv
    outputs/reports/or_utilization_report.csv
    outputs/reports/los_benchmarks.csv
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

# Assumptions for capacity modeling
# These are standard healthcare planning benchmarks
ANNUAL_CLINIC_DAYS        = 250   # working days per year
PATIENTS_PER_PHYSICIAN_PD = 20    # typical urology clinic capacity
OR_CASES_PER_DAY          = 4     # typical urology OR cases per day per surgeon
AVG_UROLOGISTS_PER_DEPT   = 8     # typical academic dept size

def main():
    print("=" * 62)
    print("  SCRIPT 10 — Module 3: Capacity & Access Analysis")
    print("=" * 62)
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    conn = sqlite3.connect(DB_PATH)

    # ── Load data ─────────────────────────────────────────
    print("[STEP 1] Loading MEPS conditions and inpatient DRG data...")
    meps      = pd.read_sql("SELECT * FROM meps_conditions", conn)
    inpatient = pd.read_sql("SELECT * FROM inpatient_drg",   conn)
    physician = pd.read_sql("SELECT state, provider_npi, total_services FROM physician_billing", conn)
    print(f"  MEPS rows      : {len(meps):,}")
    print(f"  Inpatient rows : {len(inpatient):,}")
    print(f"  Physician rows : {len(physician):,}")

    # ── Step 2: Condition demand from MEPS ────────────────
    print("\n[STEP 2] Condition demand analysis (MEPS)...")

    if 'condition_label' in meps.columns:
        demand = (meps.groupby('condition_label')
                      .size()
                      .reset_index(name='case_count')
                      .sort_values('case_count', ascending=False))

        demand['pct_of_total'] = (demand['case_count'] / demand['case_count'].sum() * 100).round(1)
        demand['demand_rank']  = range(1, len(demand)+1)

        # Classify as ambulatory (clinic) vs likely inpatient
        INPATIENT_CONDITIONS = ['Kidney Stone (Urolithiasis)', 'Acute Kidney Failure',
                                'Chronic Kidney Disease', 'Prostate Cancer']
        demand['care_setting'] = demand['condition_label'].apply(
            lambda x: 'Inpatient/Mixed' if x in INPATIENT_CONDITIONS else 'Ambulatory'
        )

        print(f"\n  CONDITION DEMAND (national survey):")
        print(f"  {'Rank':>4}  {'Condition':<35}  {'Cases':>6}  {'%':>5}  Setting")
        print(f"  {'-'*65}")
        for _, row in demand.iterrows():
            print(f"  {row['demand_rank']:>4}  {row['condition_label']:<35}  "
                  f"{row['case_count']:>6,}  {row['pct_of_total']:>4.1f}%  {row['care_setting']}")

        ambul_pct = demand[demand['care_setting']=='Ambulatory']['pct_of_total'].sum()
        print(f"\n  Ambulatory-appropriate cases : {ambul_pct:.1f}% of all urology demand")

    # ── Step 3: Inpatient procedure volumes ───────────────
    print("\n[STEP 3] Inpatient procedure volumes by DRG...")

    if 'drg_description' in inpatient.columns and 'total_discharges' in inpatient.columns:
        inpatient['total_discharges'] = pd.to_numeric(inpatient['total_discharges'], errors='coerce')

        drg_vol = (inpatient.groupby(['drg_code','drg_description'])
                             .agg(total_discharges   = ('total_discharges',    'sum'),
                                  hospital_count     = ('hospital_ccn',        'nunique'),
                                  avg_payment        = ('avg_total_payment',   'mean'),
                                  avg_medicare_pay   = ('avg_medicare_payment','mean'))
                             .reset_index()
                             .sort_values('total_discharges', ascending=False))

        print(f"\n  TOP 15 UROLOGY DRGs BY NATIONAL DISCHARGE VOLUME:")
        print(f"  {'DRG':>5}  {'Discharges':>12}  {'Hospitals':>9}  {'Avg Pay':>9}  Description")
        print(f"  {'-'*72}")
        for _, row in drg_vol.head(15).iterrows():
            desc = str(row['drg_description'])[:35]
            print(f"  {int(row['drg_code']) if pd.notna(row['drg_code']) else '?':>5}  "
                  f"{row['total_discharges']:>12,.0f}  "
                  f"{row['hospital_count']:>9,}  "
                  f"${row['avg_payment']:>8,.0f}  {desc}")

    # ── Step 4: State-level capacity analysis ─────────────
    print("\n[STEP 4] State-level capacity analysis...")

    # Count urologists per state (unique NPIs)
    urologists_per_state = (physician.groupby('state')['provider_npi']
                                     .nunique()
                                     .reset_index(name='urologist_count'))

    # Count inpatient volume per state
    if 'state' in inpatient.columns:
        inpatient_per_state = (inpatient.groupby('state')['total_discharges']
                                        .sum()
                                        .reset_index(name='total_inpatient_discharges'))

        # Ambulatory visit proxy from physician billing
        amb_per_state = (physician.groupby('state')['total_services']
                                  .sum()
                                  .reset_index(name='total_ambulatory_services'))

        # Merge into one state capacity table
        capacity = (urologists_per_state
                    .merge(inpatient_per_state, on='state', how='left')
                    .merge(amb_per_state, on='state', how='left'))

        # Estimated annual capacity per state
        # (urologists × working days × patients per day)
        capacity['annual_capacity_estimate'] = (
            capacity['urologist_count'] * ANNUAL_CLINIC_DAYS * PATIENTS_PER_PHYSICIAN_PD
        )

        # Utilization = actual services / estimated capacity
        capacity['capacity_utilization_pct'] = (
            capacity['total_ambulatory_services'] /
            capacity['annual_capacity_estimate'] * 100
        ).round(1)

        # Services per urologist (workload indicator)
        capacity['services_per_urologist'] = (
            capacity['total_ambulatory_services'] / capacity['urologist_count']
        ).round(0)

        capacity = capacity.sort_values('urologist_count', ascending=False)

        print(f"\n  STATE CAPACITY TABLE (top 15 states):")
        print(f"  {'State':>5}  {'Urologists':>10}  {'Amb. Svc':>10}  "
              f"{'Capacity%':>9}  {'Svc/MD':>8}")
        print(f"  {'-'*55}")
        for _, row in capacity.head(15).iterrows():
            flag = ' ⚠' if row['capacity_utilization_pct'] > 80 else ''
            print(f"  {row['state']:>5}  {row['urologist_count']:>10,}  "
                  f"{row['total_ambulatory_services']:>10,.0f}  "
                  f"{row['capacity_utilization_pct']:>8.1f}%  "
                  f"{row['services_per_urologist']:>8,.0f}{flag}")

        overcapacity = capacity[capacity['capacity_utilization_pct'] > 80]
        if len(overcapacity):
            print(f"\n  ⚠ States with >80% capacity utilization: "
                  f"{list(overcapacity['state'].values)}")
        else:
            print(f"\n  No states flagged at >80% utilization (model-based estimate).")

    # ── Step 5: OR utilization by DRG ─────────────────────
    print("\n[STEP 5] OR utilization analysis...")

    # OR procedures = surgical DRGs (not medical/diagnostic)
    SURGICAL_DRGS = list(range(651, 676)) + [707, 708]
    if 'drg_code' in inpatient.columns:
        inpatient['drg_code_num'] = pd.to_numeric(inpatient['drg_code'], errors='coerce')
        or_cases = inpatient[inpatient['drg_code_num'].isin(SURGICAL_DRGS)].copy()

        or_summary = (or_cases.groupby(['drg_code','drg_description'])
                               .agg(total_or_cases  = ('total_discharges',   'sum'),
                                    avg_payment     = ('avg_total_payment',  'mean'),
                                    hospital_count  = ('hospital_ccn',       'nunique'))
                               .reset_index()
                               .sort_values('total_or_cases', ascending=False))

        print(f"\n  OR UTILIZATION — UROLOGY SURGICAL DRGs:")
        print(f"  {'DRG':>5}  {'OR Cases':>10}  {'Hospitals':>9}  {'Avg Pay':>9}  Description")
        for _, row in or_summary.head(12).iterrows():
            desc = str(row['drg_description'])[:38]
            print(f"  {int(row['drg_code']) if pd.notna(row['drg_code']) else '?':>5}  "
                  f"{row['total_or_cases']:>10,.0f}  "
                  f"{row['hospital_count']:>9,}  "
                  f"${row['avg_payment']:>8,.0f}  {desc}")

        total_or = or_summary['total_or_cases'].sum()
        print(f"\n  Total national urology OR cases : {total_or:,.0f}")
        print(f"  Average payment per OR case     : ${or_summary['avg_payment'].mean():,.0f}")

    # ── Step 6: Average LOS benchmarks ────────────────────
    print("\n[STEP 6] Length of stay not directly in dataset.")
    print("  (CMS Inpatient dataset provides discharge counts and payments,")
    print("   not LOS. LOS benchmarks from HCUP Statistical Briefs:")
    print("   - Kidney/ureter procedures: avg 3.2 days")
    print("   - Prostatectomy           : avg 2.1 days")
    print("   - Urinary tract infection : avg 3.5 days (medical admission)")
    print("   These are published national averages from HCUP 2022 Brief #327)")

    los_benchmarks = pd.DataFrame({
        'procedure_category' : ['Kidney/Ureter Procedures', 'Prostatectomy',
                                'Transurethral Procedures', 'Urinary Tract Infection',
                                'Renal Failure (medical)', 'Other Kidney/Urinary'],
        'avg_los_days'       : [3.2, 2.1, 1.8, 3.5, 4.8, 3.1],
        'source'             : ['HCUP Brief 2022'] * 6,
    })

    # ── Save all outputs ───────────────────────────────────
    print("\n[STEP 7] Saving capacity reports...")

    if 'condition_label' in meps.columns:
        demand.to_csv(os.path.join(OUT_DIR, 'capacity_report.csv'), index=False)
        print(f"  Saved: capacity_report.csv  ({len(demand)} condition types)")

    if 'or_summary' in dir():
        or_summary.to_csv(os.path.join(OUT_DIR, 'or_utilization_report.csv'), index=False)
        print(f"  Saved: or_utilization_report.csv  ({len(or_summary)} DRGs)")

    los_benchmarks.to_csv(os.path.join(OUT_DIR, 'los_benchmarks.csv'), index=False)
    print(f"  Saved: los_benchmarks.csv")

    if 'capacity' in dir():
        capacity.to_csv(os.path.join(OUT_DIR, 'state_capacity.csv'), index=False)
        print(f"  Saved: state_capacity.csv  ({len(capacity)} states)")

    # ── Key findings ──────────────────────────────────────
    print()
    print("=" * 62)
    print("  KEY FINDINGS — USE THESE IN YOUR INTERVIEW")
    print("=" * 62)
    if 'demand' in dir() and len(demand):
        top_cond = demand.iloc[0]
        print(f"  #1 demand driver : {top_cond['condition_label']} ({top_cond['case_count']:,} MEPS cases)")
    if 'drg_vol' in dir() and len(drg_vol):
        top_drg = drg_vol.iloc[0]
        print(f"  #1 inpatient DRG : {top_drg['drg_description'][:45]}")
        print(f"     {top_drg['total_discharges']:,.0f} discharges nationally")
    if 'total_or' in dir():
        print(f"  Total urology OR cases nationally : {total_or:,.0f}")
    print()
    print("  POWER BI: Load capacity_report.csv + state_capacity.csv for Tab 3")
    print("  Next: python scripts/11_analysis_benchmarking.py")
    print("=" * 62)

    conn.close()
    append_audit_log('10_analysis_capacity.py', 'meps_conditions + inpatient_drg',
                     len(meps) + len(inpatient), len(demand) if 'demand' in dir() else 0,
                     'SUCCESS', 'Capacity, OR utilization, LOS benchmarks generated')

if __name__ == "__main__":
    main()
