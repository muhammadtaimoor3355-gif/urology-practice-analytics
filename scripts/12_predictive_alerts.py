"""
============================================================
Script 12 — Module 5: Predictive Alert System
============================================================
PURPOSE:
    Generates three automatic alert types using trend
    analysis and statistical thresholds.

    Alert 1 — Physician Productivity Warning
      Identifies physicians whose RVU proxy is in the
      bottom quartile — potential early warning signal.
      In a live system, this would track month-over-month
      changes; here we flag relative to peer benchmarks.

    Alert 2 — Billing Anomaly Alert
      CPT codes where billing frequency deviates >2 std dev
      from the mean across all urologists.
      Flags both high (overcoding risk) and low (underbilling).

    Alert 3 — Capacity Crisis Prediction
      States where estimated capacity utilization exceeds 80%.
      Projects how many additional physicians needed.

HOW TO RUN:
    python scripts/12_predictive_alerts.py

OUTPUT:
    outputs/reports/alerts_report.csv
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

# Capacity model constants
ANNUAL_CLINIC_DAYS        = 250
PATIENTS_PER_PHYSICIAN_PD = 20

def main():
    print("=" * 62)
    print("  SCRIPT 12 — Module 5: Predictive Alert System")
    print("=" * 62)
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    conn = sqlite3.connect(DB_PATH)
    physician = pd.read_sql("SELECT * FROM physician_billing", conn)
    conn.close()

    all_alerts = []

    # ═══════════════════════════════════════════════════
    # ALERT 1 — Physician Productivity Warning
    # ═══════════════════════════════════════════════════
    print("─" * 62)
    print("  ALERT 1 — Physician Productivity Warnings")
    print("─" * 62)

    # Aggregate to physician level
    physician['rvu_proxy'] = physician['total_services'] * physician['avg_allowed_amount']

    phys_agg = physician.groupby('provider_npi').agg(
        last_name    = ('last_name',    'first'),
        first_name   = ('first_name',   'first'),
        state        = ('state',        'first'),
        total_procs  = ('total_services','sum'),
        total_revenue= ('estimated_annual_revenue','sum'),
        rvu_proxy    = ('rvu_proxy',    'sum'),
    ).reset_index()

    p25_rvu = phys_agg['rvu_proxy'].quantile(0.25)
    p10_rvu = phys_agg['rvu_proxy'].quantile(0.10)
    median_rvu = phys_agg['rvu_proxy'].median()

    # Flag bottom 10% as RED, bottom 25% as YELLOW
    low_prod = phys_agg[phys_agg['rvu_proxy'] <= p25_rvu].copy()
    low_prod['alert_type']    = 'PRODUCTIVITY_WARNING'
    low_prod['alert_level']   = np.where(low_prod['rvu_proxy'] <= p10_rvu, 'RED', 'YELLOW')
    low_prod['rvu_shortfall'] = (median_rvu - low_prod['rvu_proxy']).round(0)
    low_prod['revenue_impact']= (low_prod['rvu_shortfall'] / median_rvu *
                                  phys_agg['total_revenue'].median()).round(2)
    low_prod['message']       = low_prod.apply(
        lambda r: f"Physician {r['last_name']}, {r['first_name']} ({r['state']}) "
                  f"RVU proxy {r['rvu_proxy']:,.0f} is "
                  f"{(r['rvu_proxy']/median_rvu*100):.0f}% of national median. "
                  f"Est. revenue gap: ${r['revenue_impact']:,.0f}/yr",
        axis=1
    )

    red_count    = (low_prod['alert_level'] == 'RED').sum()
    yellow_count = (low_prod['alert_level'] == 'YELLOW').sum()
    total_rev_gap = low_prod['revenue_impact'].sum()

    print(f"  RED alerts   (bottom 10%) : {red_count:,} physicians")
    print(f"  YELLOW alerts (10-25%)    : {yellow_count:,} physicians")
    print(f"  Total estimated revenue gap: ${total_rev_gap:,.0f}")
    print(f"\n  Sample RED alerts:")
    red_sample = low_prod[low_prod['alert_level']=='RED'].head(5)
    print(red_sample[['last_name','first_name','state','total_procs',
                       'rvu_proxy','revenue_impact','alert_level']].to_string(index=False))

    alert1_cols = ['provider_npi','last_name','first_name','state',
                   'total_procs','total_revenue','rvu_proxy',
                   'alert_type','alert_level','rvu_shortfall','revenue_impact','message']
    all_alerts.append(low_prod[alert1_cols])

    # ═══════════════════════════════════════════════════
    # ALERT 2 — Billing Anomaly Alert
    # ═══════════════════════════════════════════════════
    print(f"\n{'─'*62}")
    print("  ALERT 2 — Billing Anomaly Alerts")
    print("─" * 62)

    # For each CPT code: mean and std of services across physicians
    cpt_stats = (physician.groupby('cpt_code')['total_services']
                           .agg(['mean','std','count'])
                           .reset_index())
    cpt_stats.columns = ['cpt_code','mean_srvcs','std_srvcs','physician_count']
    cpt_stats['upper_2sd'] = cpt_stats['mean_srvcs'] + 2 * cpt_stats['std_srvcs']

    # Only flag codes billed by >10 physicians (avoid rare codes)
    cpt_stats = cpt_stats[cpt_stats['physician_count'] >= 10]

    df_merged = physician.merge(cpt_stats, on='cpt_code', how='inner')
    anomalies = df_merged[df_merged['total_services'] > df_merged['upper_2sd']].copy()
    anomalies['z_score']     = ((anomalies['total_services'] - anomalies['mean_srvcs'])
                                / anomalies['std_srvcs']).round(2)
    anomalies['alert_type']  = 'BILLING_ANOMALY'
    anomalies['alert_level'] = np.where(anomalies['z_score'] > 4, 'RED', 'YELLOW')
    anomalies['message']     = anomalies.apply(
        lambda r: f"CPT {r['cpt_code']} ({str(r['cpt_description'])[:30]}): "
                  f"Dr. {r['last_name']} billed {r['total_services']:,.0f} "
                  f"vs peer mean {r['mean_srvcs']:.0f} (z={r['z_score']})",
        axis=1
    )

    red_bill    = (anomalies['alert_level'] == 'RED').sum()
    yellow_bill = (anomalies['alert_level'] == 'YELLOW').sum()
    print(f"  RED anomalies (z>4)    : {red_bill:,}")
    print(f"  YELLOW anomalies (z>2) : {yellow_bill:,}")
    print(f"\n  Top 5 most extreme anomalies (highest z-score):")
    top_anom = anomalies.nlargest(5, 'z_score')[
        ['last_name','state','cpt_code','cpt_description','total_services',
         'mean_srvcs','z_score','alert_level']
    ]
    print(top_anom.to_string(index=False))

    alert2_cols = ['provider_npi','last_name','state','cpt_code','cpt_description',
                   'total_services','mean_srvcs','z_score',
                   'alert_type','alert_level','message']
    all_alerts.append(anomalies[alert2_cols].rename(
        columns={'cpt_description':'message_detail'}
    ).assign(
        first_name='', rvu_proxy=np.nan, rvu_shortfall=np.nan,
        revenue_impact=np.nan, total_procs=anomalies['total_services'],
        total_revenue=anomalies['estimated_annual_revenue'],
        message=anomalies['message']
    )[alert1_cols])

    # ═══════════════════════════════════════════════════
    # ALERT 3 — Capacity Crisis Prediction
    # ═══════════════════════════════════════════════════
    print(f"\n{'─'*62}")
    print("  ALERT 3 — Capacity Crisis Predictions")
    print("─" * 62)

    state_vol = (physician.groupby('state')
                          .agg(urologist_count = ('provider_npi',   'nunique'),
                               total_services  = ('total_services', 'sum'))
                          .reset_index())

    state_vol['annual_capacity'] = (
        state_vol['urologist_count'] * ANNUAL_CLINIC_DAYS * PATIENTS_PER_PHYSICIAN_PD
    )
    state_vol['utilization_pct'] = (
        state_vol['total_services'] / state_vol['annual_capacity'] * 100
    ).round(1)
    state_vol['over_capacity'] = state_vol['utilization_pct'] > 100

    # How many additional MDs needed to get to 80% utilization
    state_vol['additional_mds_needed'] = (
        (state_vol['total_services'] / (0.80 * ANNUAL_CLINIC_DAYS * PATIENTS_PER_PHYSICIAN_PD))
        - state_vol['urologist_count']
    ).clip(lower=0).round(0).astype(int)

    crisis_states = state_vol[state_vol['utilization_pct'] > 80].sort_values(
        'utilization_pct', ascending=False
    )

    crisis_alerts = crisis_states.copy()
    crisis_alerts['alert_type']  = 'CAPACITY_CRISIS'
    crisis_alerts['alert_level'] = np.where(
        crisis_alerts['utilization_pct'] > 150, 'RED', 'YELLOW'
    )
    crisis_alerts['message'] = crisis_alerts.apply(
        lambda r: f"State {r['state']}: capacity at {r['utilization_pct']:.0f}%. "
                  f"Need {r['additional_mds_needed']} more urologists to reach 80% target.",
        axis=1
    )

    red_cap    = (crisis_alerts['alert_level'] == 'RED').sum()
    yellow_cap = (crisis_alerts['alert_level'] == 'YELLOW').sum()
    print(f"  RED capacity alerts (>150%)   : {red_cap} states")
    print(f"  YELLOW capacity alerts (>80%) : {yellow_cap} states")
    print(f"\n  TOP 10 CAPACITY-STRESSED STATES:")
    print(f"  {'State':>5}  {'Utilization':>11}  {'Add. MDs':>8}  Level")
    for _, row in crisis_alerts.head(10).iterrows():
        print(f"  {row['state']:>5}  {row['utilization_pct']:>10.1f}%  "
              f"{row['additional_mds_needed']:>8,}  {row['alert_level']}")

    # Add to alerts
    cap_alert_df = crisis_alerts[['state','utilization_pct','additional_mds_needed',
                                   'alert_type','alert_level','message']].copy()
    cap_alert_df['provider_npi']   = 'N/A'
    cap_alert_df['last_name']      = cap_alert_df['state']
    cap_alert_df['first_name']     = ''
    cap_alert_df['total_procs']    = crisis_alerts['total_services']
    cap_alert_df['total_revenue']  = np.nan
    cap_alert_df['rvu_proxy']      = np.nan
    cap_alert_df['rvu_shortfall']  = np.nan
    cap_alert_df['revenue_impact'] = np.nan
    all_alerts.append(cap_alert_df[alert1_cols])

    # ═══════════════════════════════════════════════════
    # SAVE ALL ALERTS
    # ═══════════════════════════════════════════════════
    alerts_df = pd.concat(all_alerts, ignore_index=True)
    alerts_df.to_csv(os.path.join(OUT_DIR, 'alerts_report.csv'), index=False)
    print(f"\n  Saved: alerts_report.csv  ({len(alerts_df):,} total alerts)")

    total_red    = (alerts_df['alert_level'] == 'RED').sum()
    total_yellow = (alerts_df['alert_level'] == 'YELLOW').sum()

    print()
    print("=" * 62)
    print("  ALERT SYSTEM SUMMARY")
    print("=" * 62)
    print(f"  Total RED alerts    : {total_red:,}")
    print(f"  Total YELLOW alerts : {total_yellow:,}")
    print(f"  Alert 1 (Productivity) : {len(low_prod):,} physicians flagged")
    print(f"  Alert 2 (Billing)      : {len(anomalies):,} CPT anomalies")
    print(f"  Alert 3 (Capacity)     : {len(crisis_alerts):,} states at risk")
    print(f"  Est. revenue at risk   : ${total_rev_gap:,.0f}")
    print()
    print("  POWER BI: Load alerts_report.csv for Tab 5")
    print("  Next: python scripts/13_forecasting_arima.py")
    print("=" * 62)

    append_audit_log('12_predictive_alerts.py', 'physician_billing',
                     len(physician), len(alerts_df), 'SUCCESS',
                     f"{total_red} RED + {total_yellow} YELLOW alerts generated")

if __name__ == "__main__":
    main()
