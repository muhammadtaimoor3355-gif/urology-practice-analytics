"""
run_all.py — Master Pipeline Runner
Johns Hopkins Urology Analytics System

Runs all 18 scripts in order. Safe to re-run at any time.
Usage:
    python run_all.py
    python run_all.py --start 08   # resume from script 08
"""

import os
import sys
import subprocess
import argparse
from datetime import datetime

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR  = os.path.join(PROJECT_ROOT, 'scripts')

PIPELINE = [
    ('00', '_00_setup_environment.py',       'Setup folders & audit trail'),
    ('01', '01_download_cms_data.py',         'Download CMS physician billing (162K rows)'),
    ('02', '02_download_hcup_data.py',        'Download CMS inpatient DRG data (14K rows)'),
    ('03', '03_download_meps_data.py',        'Download MEPS conditions H241 (2.7K rows)'),
    ('04', '04_download_hospital_compare.py', 'Download CMS hospital quality ratings'),
    ('05', '05_download_benchmarks.py',       'Download CMS geography benchmarks'),
    ('06', '06_clean_and_validate.py',        'Clean & validate all datasets'),
    ('07', '07_load_to_sqlite.py',            'Load to SQLite database (63.5 MB)'),
    ('08', '08_analysis_productivity.py',     'Physician productivity analysis'),
    ('09', '09_analysis_billing.py',          'Billing & CPT code analysis'),
    ('10', '10_analysis_capacity.py',         'Capacity & access gap analysis'),
    ('11', '11_analysis_benchmarking.py',     'National benchmarking (MD vs US)'),
    ('12', '12_predictive_alerts.py',         'Predictive alert system (3 alert types)'),
    ('13', '13_forecasting_arima.py',         'ARIMA 6-month forecast'),
    ('14', '14_scenario_modeling.py',         'What-if scenario modeling (4 scenarios)'),
    ('15', '15_generate_powerbi_exports.py',  'Generate Power BI CSV exports'),
    ('16', '16_generate_executive_memo.py',   'Generate executive memo'),
    ('17', '17_run_audit_report.py',          'Final audit & completion check'),
]

def run_script(script_file, label):
    path = os.path.join(SCRIPTS_DIR, script_file)
    result = subprocess.run(
        [sys.executable, path],
        cwd=PROJECT_ROOT,
        capture_output=False,
    )
    return result.returncode == 0

def main():
    parser = argparse.ArgumentParser(description='Run urology analytics pipeline')
    parser.add_argument('--start', default='00', help='Script number to start from (e.g. 08)')
    args = parser.parse_args()

    print("=" * 70)
    print("  JOHNS HOPKINS UROLOGY ANALYTICS — MASTER PIPELINE")
    print("=" * 70)
    print(f"  Started : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Scripts : {len(PIPELINE)} total")
    if args.start != '00':
        print(f"  Resuming from script {args.start}")
    print()

    results = []
    start_running = False

    for num, script, label in PIPELINE:
        if num == args.start or args.start == '00':
            start_running = True
        if not start_running:
            print(f"  [SKIP] {num} — {label}")
            continue

        print(f"\n{'─'*70}")
        print(f"  RUNNING [{num}] {label}")
        print(f"{'─'*70}")
        t0 = datetime.now()
        ok = run_script(script, label)
        elapsed = (datetime.now() - t0).total_seconds()
        status = 'OK' if ok else 'FAILED'
        results.append((num, label, status, round(elapsed, 1)))

        if not ok:
            print(f"\n  ✗ SCRIPT {num} FAILED — pipeline stopped.")
            print(f"  Fix the error above then re-run: python run_all.py --start {num}")
            sys.exit(1)

    print()
    print("=" * 70)
    print("  PIPELINE COMPLETE")
    print("=" * 70)
    print(f"  {'Script':<6}  {'Status':>6}  {'Time(s)':>8}  Description")
    print(f"  {'─'*60}")
    total_time = 0
    for num, label, status, elapsed in results:
        mark = '✓' if status == 'OK' else '✗'
        print(f"  {mark} [{num}]  {status:>6}  {elapsed:>8.1f}s  {label}")
        total_time += elapsed
    print(f"\n  Total runtime : {total_time:.0f} seconds ({total_time/60:.1f} minutes)")
    print(f"  Finished at   : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    print("  DELIVERABLES READY:")
    print("  • outputs/powerbi/      ← 17 Power BI CSV files")
    print("  • outputs/reports/      ← analysis CSVs + executive memo")
    print("  • data/processed/       ← master_database.sqlite (63.5 MB)")
    print("=" * 70)

if __name__ == "__main__":
    main()
