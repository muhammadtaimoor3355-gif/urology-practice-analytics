"""
============================================================
Script 15 — Power BI Export Generator
============================================================
PURPOSE:
    Consolidates all analysis outputs into a single folder
    (outputs/powerbi/) with standardized, Power-BI-ready
    CSV files.  Also writes a data dictionary so the
    dashboard builder knows every column.

HOW TO RUN:
    python scripts/15_generate_powerbi_exports.py

OUTPUT:
    outputs/powerbi/  — one CSV per dashboard tab + dict
============================================================
"""

import os
import sys
import shutil
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

REPORTS_DIR = os.path.join(PROJECT_ROOT, 'outputs', 'reports')
PBI_DIR     = os.path.join(PROJECT_ROOT, 'outputs', 'powerbi')
DB_PATH     = os.path.join(PROJECT_ROOT, 'data', 'processed', 'master_database.sqlite')
os.makedirs(PBI_DIR, exist_ok=True)

# ── Mapping: source CSV → Power BI file name ─────────────
COPY_MAP = {
    'productivity_report.csv'  : 'tab1_physician_productivity.csv',
    'billing_report.csv'       : 'tab2_billing_analysis.csv',
    'billing_anomalies.csv'    : 'tab2_billing_anomalies.csv',
    'capacity_report.csv'      : 'tab3_condition_demand.csv',
    'state_capacity.csv'       : 'tab3_state_capacity.csv',
    'or_utilization_report.csv': 'tab3_or_utilization.csv',
    'benchmark_report.csv'     : 'tab4_benchmark_report.csv',
    'state_rankings.csv'       : 'tab4_state_rankings.csv',
    'performance_scorecard.csv': 'tab4_scorecard.csv',
    'alerts_report.csv'        : 'tab5_alerts.csv',
    'forecast_report.csv'      : 'tab5_forecast.csv',
    'scenario_report.csv'      : 'tab6_scenarios.csv',
    'los_benchmarks.csv'       : 'tab3_los_benchmarks.csv',
    'productivity_by_state.csv': 'tab1_productivity_by_state.csv',
    'cpt_benchmarks.csv'       : 'tab2_cpt_benchmarks.csv',
}

def load_report(filename):
    path = os.path.join(REPORTS_DIR, filename)
    if os.path.exists(path):
        return pd.read_csv(path)
    return None

def main():
    print("=" * 62)
    print("  SCRIPT 15 — Power BI Export Generator")
    print("=" * 62)
    print(f"  Started : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Output  : {PBI_DIR}")
    print()

    exported = []

    # ── Step 1: Copy & rename existing reports ────────────
    print("[STEP 1] Copying analysis reports to Power BI folder...")
    for src_name, dst_name in COPY_MAP.items():
        src = os.path.join(REPORTS_DIR, src_name)
        dst = os.path.join(PBI_DIR, dst_name)
        if os.path.exists(src):
            shutil.copy2(src, dst)
            df = pd.read_csv(dst)
            print(f"  ✓  {dst_name:<45} {len(df):>7,} rows")
            exported.append({'file': dst_name, 'rows': len(df), 'tab': dst_name.split('_')[0]})
        else:
            print(f"  ✗  {src_name} NOT FOUND — skipping")

    # ── Step 2: Build summary table for Tab 0 (Overview) ──
    print("\n[STEP 2] Building executive overview table...")

    conn = sqlite3.connect(DB_PATH)
    physician = pd.read_sql("SELECT state, provider_npi, total_services, "
                            "estimated_annual_revenue FROM physician_billing", conn)
    conn.close()

    overview = pd.DataFrame([
        {'kpi': 'Total urologists tracked',         'value': f"{physician['provider_npi'].nunique():,}",       'source': 'CMS Physician Billing 2022'},
        {'kpi': 'Total urology procedures (US)',    'value': f"{physician['total_services'].sum():,.0f}",      'source': 'CMS Physician Billing 2022'},
        {'kpi': 'Total estimated revenue (US)',     'value': f"${physician['estimated_annual_revenue'].sum():,.0f}", 'source': 'CMS Physician Billing 2022'},
        {'kpi': 'National median revenue/physician','value': f"${physician.groupby('provider_npi')['estimated_annual_revenue'].sum().median():,.0f}", 'source': 'CMS Physician Billing 2022'},
        {'kpi': 'States analyzed',                  'value': f"{physician['state'].nunique()}",                 'source': 'CMS Physician Billing 2022'},
        {'kpi': 'Maryland avg revenue vs national', 'value': '+45.7%',                                          'source': 'Script 11 benchmarking'},
        {'kpi': 'Maryland revenue rank',            'value': '#6 of 51 states',                                 'source': 'Script 11 benchmarking'},
        {'kpi': '#1 urology demand condition',      'value': 'Urinary Tract Infection (27.5%)',                 'source': 'MEPS H241 2022'},
        {'kpi': '6-month forecast procedures',      'value': '19,757,037',                                      'source': 'Script 13 ARIMA forecast'},
        {'kpi': '6-month forecast revenue',         'value': '$550,249,736',                                    'source': 'Script 13 ARIMA forecast'},
        {'kpi': 'Total RED alerts generated',       'value': '2,372',                                           'source': 'Script 12 alert system'},
        {'kpi': 'Revenue at risk (productivity)',   'value': '$161.8M',                                         'source': 'Script 12 alert system'},
        {'kpi': 'ROI per new physician hired',      'value': '$706,306/yr',                                     'source': 'Script 14 scenario model'},
        {'kpi': 'OR revenue gain (10 slots/wk)',    'value': '+$1,294,032/yr',                                  'source': 'Script 14 scenario model'},
        {'kpi': '5% Medicare cut impact',           'value': '-$308,403/yr',                                    'source': 'Script 14 scenario model'},
    ])

    overview_path = os.path.join(PBI_DIR, 'tab0_executive_overview.csv')
    overview.to_csv(overview_path, index=False)
    print(f"  Saved: tab0_executive_overview.csv  ({len(overview)} KPIs)")
    exported.append({'file': 'tab0_executive_overview.csv', 'rows': len(overview), 'tab': 'tab0'})

    # ── Step 3: Build data dictionary ─────────────────────
    print("\n[STEP 3] Building data dictionary...")

    data_dict_rows = [
        # Tab 0
        ('tab0_executive_overview', 'kpi',           'string',  'KPI name'),
        ('tab0_executive_overview', 'value',          'string',  'KPI value (formatted)'),
        ('tab0_executive_overview', 'source',         'string',  'Data source / script'),
        # Tab 1
        ('tab1_physician_productivity', 'provider_npi',           'string',  'CMS National Provider Identifier'),
        ('tab1_physician_productivity', 'last_name',              'string',  'Physician last name'),
        ('tab1_physician_productivity', 'first_name',             'string',  'Physician first name'),
        ('tab1_physician_productivity', 'state',                  'string',  '2-letter state code'),
        ('tab1_physician_productivity', 'total_procedures',       'int',     'Total Medicare procedures billed'),
        ('tab1_physician_productivity', 'estimated_annual_revenue','float',  'Estimated revenue (procedures × avg payment)'),
        ('tab1_physician_productivity', 'rvu_proxy',              'float',   'RVU proxy (services × avg allowed amount)'),
        ('tab1_physician_productivity', 'performance_tier',       'string',  'TOP_10 / TOP_25 / ABOVE_MEDIAN / BELOW_MEDIAN / BOTTOM_25'),
        # Tab 2
        ('tab2_billing_analysis', 'cpt_code',         'string',  'CPT / HCPCS procedure code'),
        ('tab2_billing_analysis', 'cpt_description',  'string',  'Procedure description'),
        ('tab2_billing_analysis', 'total_services',   'int',     'National total services billed'),
        ('tab2_billing_analysis', 'total_revenue',    'float',   'Estimated total revenue for this CPT'),
        ('tab2_billing_analysis', 'physician_count',  'int',     'Number of physicians billing this code'),
        ('tab2_billing_anomalies','z_score',           'float',   'Z-score vs peer mean (|z|>2 = anomaly)'),
        ('tab2_billing_anomalies','alert_level',       'string',  'RED (z>4) or YELLOW (z>2)'),
        # Tab 3
        ('tab3_state_capacity',   'state',            'string',  '2-letter state code'),
        ('tab3_state_capacity',   'urologist_count',  'int',     'Unique urologists (NPIs) in state'),
        ('tab3_state_capacity',   'annual_capacity_estimate', 'int', 'urologists × 250 days × 20 pts/day'),
        ('tab3_state_capacity',   'capacity_utilization_pct','float','Ambulatory services / capacity × 100'),
        # Tab 4
        ('tab4_state_rankings',   'state',            'string',  '2-letter state code'),
        ('tab4_state_rankings',   'avg_revenue_per_md','float',  'Average annual revenue per urologist'),
        ('tab4_state_rankings',   'revenue_per_md_rank','int',   'Rank 1=highest revenue/MD'),
        ('tab4_scorecard',        'metric',           'string',  'Scorecard metric name'),
        ('tab4_scorecard',        'value',            'string',  'Observed value'),
        ('tab4_scorecard',        'benchmark',        'string',  'Comparison benchmark'),
        ('tab4_scorecard',        'status',           'string',  'ABOVE / BELOW / INFO / WARNING'),
        # Tab 5
        ('tab5_alerts',           'alert_type',       'string',  'PRODUCTIVITY_WARNING / BILLING_ANOMALY / CAPACITY_CRISIS'),
        ('tab5_alerts',           'alert_level',      'string',  'RED or YELLOW'),
        ('tab5_alerts',           'message',          'string',  'Human-readable alert description'),
        ('tab5_forecast',         'metric',           'string',  'Patient Volume / Revenue / Inpatient Discharges'),
        ('tab5_forecast',         'forecast_month',   'string',  'YYYY-MM forecast period'),
        ('tab5_forecast',         'forecast_value',   'float',   'ARIMA(1,1,1) point forecast'),
        ('tab5_forecast',         'lower_80pct_ci',   'float',   'Lower 80% confidence interval'),
        ('tab5_forecast',         'upper_80pct_ci',   'float',   'Upper 80% confidence interval'),
        # Tab 6
        ('tab6_scenarios',        'scenario',         'string',  'Scenario name (BASELINE / A / B / C / D)'),
        ('tab6_scenarios',        'physicians',       'int',     'Number of physicians in scenario'),
        ('tab6_scenarios',        'total_annual_revenue','float','Projected annual revenue'),
        ('tab6_scenarios',        'vs_baseline_revenue','float', 'Revenue delta vs baseline'),
        ('tab6_scenarios',        'capacity_utilization_pct','float','Capacity utilization %'),
        ('tab6_scenarios',        'is_baseline',      'bool',    'True for the current-state baseline row'),
    ]

    data_dict = pd.DataFrame(data_dict_rows,
                             columns=['table','column','data_type','description'])
    dict_path = os.path.join(PBI_DIR, 'data_dictionary.csv')
    data_dict.to_csv(dict_path, index=False)
    print(f"  Saved: data_dictionary.csv  ({len(data_dict)} column definitions)")

    # ── Step 4: Print Power BI load order ─────────────────
    print()
    print("=" * 62)
    print("  POWER BI LOAD ORDER")
    print("=" * 62)
    tabs = {
        'tab0': ('Overview KPIs',         'tab0_executive_overview.csv'),
        'tab1': ('Physician Productivity','tab1_physician_productivity.csv'),
        'tab2': ('Billing Analysis',      'tab2_billing_analysis.csv'),
        'tab3': ('Capacity & Access',     'tab3_state_capacity.csv'),
        'tab4': ('National Benchmarks',   'tab4_scorecard.csv'),
        'tab5': ('Alerts & Forecast',     'tab5_alerts.csv'),
        'tab6': ('Scenario Modeling',     'tab6_scenarios.csv'),
    }
    for tab_id, (tab_name, primary_file) in tabs.items():
        print(f"  {tab_id.upper()} — {tab_name:<25}  ← {primary_file}")

    print()
    print(f"  All files in: {PBI_DIR}")
    print(f"  Total files exported: {len(exported) + 1}")  # +1 for data dict

    print()
    print("=" * 62)
    print("  SCRIPT 15 COMPLETE")
    print("  Next: python scripts/16_generate_executive_memo.py")
    print("=" * 62)

    append_audit_log('15_generate_powerbi_exports.py', 'All outputs',
                     0, len(exported), 'SUCCESS',
                     f"{len(exported)} Power BI files exported to outputs/powerbi/")

if __name__ == "__main__":
    main()
