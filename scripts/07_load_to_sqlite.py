"""
============================================================
Script 07 — Load All Clean Data into SQLite Database
============================================================
PURPOSE:
    Loads all 5 cleaned CSV files into a single SQLite
    database file: data/processed/master_database.sqlite

    WHY SQLITE?
    SQLite is a real database that lives in a single file.
    Instead of loading CSVs every time an analysis script
    runs, we store everything once in the database and
    query it with SQL. This is:
      - Much faster for large datasets
      - More professional (shows database skills)
      - Allows JOIN queries across tables
        (e.g. link physician billing to hospital quality)
      - The same approach used in hospital data warehouses

    TABLES CREATED:
      physician_billing  — Script 01 data (162K rows)
      inpatient_drg      — Script 02 data (13K rows)
      meps_conditions    — Script 03 data (2.7K rows)
      hospital_compare   — Script 04 data (5.4K rows)
      benchmarks_geo     — Script 05 data (11.7K rows)

    INDEXES CREATED (makes queries faster):
      - physician_billing: provider_npi, state, cpt_code
      - inpatient_drg: drg_code, state
      - benchmarks_geo: cpt_code, geo_level

HOW TO RUN:
    python scripts/07_load_to_sqlite.py

OUTPUT:
    data/processed/master_database.sqlite
============================================================
"""

import os
import sys
import sqlite3
import pandas as pd
from datetime import datetime

# ── Project root ────────────────────────────────────────────
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.join(PROJECT_ROOT, 'scripts'))

try:
    from _00_setup_environment import append_audit_log
except ImportError:
    def append_audit_log(*a, **k): pass

# ── Paths ───────────────────────────────────────────────────
PROC   = os.path.join(PROJECT_ROOT, 'data', 'processed')
DB_PATH = os.path.join(PROC, 'master_database.sqlite')

# Each entry: (csv_filename, table_name_in_db)
DATASETS = [
    ('physician_clean.csv',  'physician_billing'),
    ('inpatient_clean.csv',  'inpatient_drg'),
    ('meps_clean.csv',       'meps_conditions'),
    ('hospital_clean.csv',   'hospital_compare'),
    ('benchmarks_clean.csv', 'benchmarks_geo'),
]

# Indexes to create after loading (column, table)
# These make SELECT and WHERE queries much faster
INDEXES = [
    ('physician_billing', 'provider_npi'),
    ('physician_billing', 'state'),
    ('physician_billing', 'cpt_code'),
    ('inpatient_drg',     'drg_code'),
    ('inpatient_drg',     'state'),
    ('benchmarks_geo',    'cpt_code'),
    ('benchmarks_geo',    'geo_level'),
    ('hospital_compare',  'state'),
]

# ── Main ────────────────────────────────────────────────────

def main():
    print("=" * 62)
    print("  SCRIPT 07 — Load Clean Data into SQLite Database")
    print("=" * 62)
    print(f"  Started  : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Database : {DB_PATH}")
    print()

    # Remove existing database so we start fresh
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
        print("  Removed existing database — creating fresh copy.")

    conn = sqlite3.connect(DB_PATH)
    total_rows = 0

    # ── Load each CSV into a database table ──────────────
    for csv_file, table_name in DATASETS:
        csv_path = os.path.join(PROC, csv_file)

        if not os.path.exists(csv_path):
            print(f"  SKIP: {csv_file} not found — run Script 06 first.")
            continue

        print(f"  Loading {csv_file} → table '{table_name}'...")
        df = pd.read_csv(csv_path, low_memory=False)
        print(f"    Rows: {len(df):,}  |  Columns: {len(df.columns)}")

        # Write to SQLite — if_exists='replace' overwrites any old table
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        total_rows += len(df)
        print(f"    Loaded to database.")

    # ── Create indexes for faster queries ─────────────────
    print(f"\n  Creating query indexes...")
    cursor = conn.cursor()
    for table, col in INDEXES:
        try:
            idx_name = f"idx_{table}_{col}"
            cursor.execute(
                f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table} ({col})"
            )
            print(f"    INDEX: {idx_name}")
        except sqlite3.OperationalError as e:
            print(f"    SKIP index {table}.{col}: {e}")

    conn.commit()

    # ── Verify by querying each table ─────────────────────
    print(f"\n  Verifying database contents...")
    print(f"  {'Table':<22} {'Rows':>8}  {'Columns':>8}")
    print(f"  {'-'*40}")

    for _, table_name in DATASETS:
        try:
            count   = pd.read_sql(f"SELECT COUNT(*) as n FROM {table_name}", conn)['n'][0]
            cols    = pd.read_sql(f"SELECT * FROM {table_name} LIMIT 1", conn).shape[1]
            print(f"  {table_name:<22} {count:>8,}  {cols:>8}")
        except Exception as e:
            print(f"  {table_name:<22} ERROR: {e}")

    # ── Run a sample JOIN query to prove it works ─────────
    print(f"\n  Sample cross-table query (physician + benchmarks):")
    print(f"  'Top 5 CPT codes: physician volume vs national average'")
    try:
        sample_sql = """
        SELECT
            p.cpt_code,
            p.cpt_description,
            SUM(p.total_services)          AS dept_total_services,
            AVG(p.avg_medicare_payment)    AS dept_avg_payment,
            b.total_services               AS national_total_services,
            b.avg_medicare_payment         AS national_avg_payment
        FROM physician_billing p
        LEFT JOIN benchmarks_geo b
            ON p.cpt_code = b.cpt_code
           AND b.geo_level = 'National'
           AND b.place_of_service = p.place_of_service
        GROUP BY p.cpt_code, p.cpt_description
        ORDER BY dept_total_services DESC
        LIMIT 5
        """
        result = pd.read_sql(sample_sql, conn)
        pd.set_option('display.max_colwidth', 40)
        pd.set_option('display.width', 120)
        print(result.to_string(index=False))
    except Exception as e:
        print(f"  Sample query error: {e}")

    conn.close()

    # ── Final summary ─────────────────────────────────────
    db_size = os.path.getsize(DB_PATH) / 1024 / 1024
    print()
    print("=" * 62)
    print("  SCRIPT 07 COMPLETE")
    print(f"  Database  : {DB_PATH}")
    print(f"  DB size   : {db_size:.2f} MB")
    print(f"  Total rows: {total_rows:,}")
    print(f"  Tables    : {len(DATASETS)}")
    print()
    print("  You can now open the database in:")
    print("  - DB Browser for SQLite (free GUI tool)")
    print("  - Any Python script using: sqlite3.connect(DB_PATH)")
    print("  - Power BI (via ODBC connector)")
    print()
    print("  Next: python scripts/08_analysis_productivity.py")
    print("=" * 62)

    append_audit_log('07_load_to_sqlite.py', 'SQLite master database',
                     total_rows, total_rows, 'SUCCESS',
                     f"5 tables, {db_size:.1f} MB database created")


if __name__ == "__main__":
    main()
