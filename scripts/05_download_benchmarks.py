"""
============================================================
Script 05 — Download National Urology Benchmark Data
============================================================
PURPOSE:
    Downloads the CMS Medicare Physician & Other Practitioners
    by Geography and Service dataset (2022).

    This is DIFFERENT from Script 01 (which is physician-level).
    This dataset aggregates billing data by STATE + CPT CODE,
    giving us:
      - National average payment per urology procedure
      - State-by-state average payments
      - National average procedure volumes
      - Average submitted charges vs allowed amounts

    WHY THIS IS THE BENCHMARK SOURCE:
    In Module 4 (National Benchmarking), we compare a specific
    department's metrics against these state/national averages.
    For example: "Maryland urologists billed CPT 52000
    (cystoscopy) at $X average — Johns Hopkins is at $Y."

    The "by Geography" dataset is the standard benchmark
    reference used in health services research for exactly
    this purpose.

DATA SOURCE:
    CMS Medicare Physician & Other Practitioners
    by Geography and Service — 2022 Data Year
    GUID: 87304f15-9ed0-41dc-a141-6141a0327453

HOW TO RUN:
    python scripts/05_download_benchmarks.py

OUTPUT:
    data/raw/benchmarks/cms_geo_urology_2022.csv
    data/raw/benchmarks/benchmarks_metadata.json
============================================================
"""

import os
import sys
import json
import time
import requests
import pandas as pd
from datetime import datetime

# ── Project root ──────────────────────────────────────────
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.join(PROJECT_ROOT, 'scripts'))

try:
    from _00_setup_environment import append_audit_log
except ImportError:
    def append_audit_log(*a, **k): pass

# ── Config ────────────────────────────────────────────────
CMS_API_BASE     = "https://data.cms.gov/data-api/v1/dataset"
CMS_DATASET_GUID = "87304f15-9ed0-41dc-a141-6141a0327453"  # 2022 by Geography & Service
PAGE_SIZE        = 5000
REQUEST_DELAY    = 0.3

OUTPUT_DIR  = os.path.join(PROJECT_ROOT, 'data', 'raw', 'benchmarks')
OUTPUT_CSV  = os.path.join(OUTPUT_DIR, 'cms_geo_urology_2022.csv')
OUTPUT_META = os.path.join(OUTPUT_DIR, 'benchmarks_metadata.json')

BASE_URL = f"{CMS_API_BASE}/{CMS_DATASET_GUID}/data"

# Urology keywords to filter DRG/procedure descriptions
UROLOGY_KEYWORDS = [
    'urol', 'kidney', 'renal', 'bladder', 'prostat', 'ureter',
    'ureth', 'cystoscop', 'nephrect', 'lithotrip', 'ureteral',
    'catheter', 'transurethral', 'incontinence'
]

# ── Helpers ───────────────────────────────────────────────

def probe_columns():
    """Fetch 1 row to see column names and find specialty/type column."""
    print("[STEP 1] Probing API for column names and specialty column...")
    r = requests.get(BASE_URL, params={"size": 1, "offset": 0}, timeout=30)
    r.raise_for_status()
    data = r.json()
    row  = data[0] if isinstance(data, list) else data.get('data', [{}])[0]
    cols = list(row.keys())
    print(f"  Columns ({len(cols)}): {cols}")

    # Find specialty/type column
    spec_col = None
    for col in cols:
        if col in ('Rndrng_Prvdr_Type', 'RFRG_Prvdr_Type', 'Prvdr_Type'):
            spec_col = col
            break
    if not spec_col:
        for col in cols:
            if 'type' in col.lower() or 'spclty' in col.lower():
                spec_col = col
                break

    # Find HCPCS/CPT description column
    desc_col = None
    for col in cols:
        if 'desc' in col.lower() and 'hcpcs' in col.lower():
            desc_col = col
            break

    # Find geography column
    geo_col = None
    for col in cols:
        if 'state' in col.lower() or 'geo' in col.lower():
            geo_col = col
            break

    print(f"  Specialty column : {spec_col}")
    print(f"  Description col  : {desc_col}")
    print(f"  Geography column : {geo_col}")
    return cols, spec_col, desc_col, geo_col


def fetch_page_filtered(offset, spec_col, specialty_val):
    """
    Download one page, filtering server-side by specialty if possible.
    Uses params dict for proper URL encoding (lesson from Script 01).
    """
    params = {"size": PAGE_SIZE, "offset": offset}
    if spec_col and specialty_val:
        params[f"filter[{spec_col}]"] = specialty_val

    for attempt in range(1, 4):
        try:
            r = requests.get(BASE_URL, params=params, timeout=60)
            r.raise_for_status()
            data = r.json()
            if isinstance(data, list):
                return data
            if isinstance(data, dict) and 'data' in data:
                return data['data']
            return []
        except Exception as e:
            if attempt == 3:
                print(f"  ERROR at offset {offset}: {e}")
                return []
            time.sleep(3 * attempt)
    return []


def is_urology_row(row, spec_col, desc_col):
    """Return True if this row relates to urology."""
    # Check specialty type
    if spec_col:
        val = str(row.get(spec_col, '')).lower()
        if 'urol' in val:
            return True
    # Check HCPCS description
    if desc_col:
        val = str(row.get(desc_col, '')).lower()
        if any(kw in val for kw in UROLOGY_KEYWORDS):
            return True
    return False


# ── Main ──────────────────────────────────────────────────

def main():
    print("=" * 62)
    print("  SCRIPT 05 — National Urology Benchmark Data Download")
    print("=" * 62)
    print(f"  Started : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Source  : CMS Medicare by Geography & Service 2022")
    print()

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Skip if already done
    if os.path.exists(OUTPUT_CSV):
        size_mb = os.path.getsize(OUTPUT_CSV) / 1024 / 1024
        print(f"  File already exists ({size_mb:.1f} MB).")
        choice = input("  Re-download? (y/n): ").strip().lower()
        if choice != 'y':
            print("  Using existing file.")
            return

    # Step 1 — probe columns
    cols, spec_col, desc_col, geo_col = probe_columns()

    # Step 2 — probe specialty value
    specialty_val = None
    if spec_col:
        print(f"\n[STEP 2] Finding exact 'Urology' label in {spec_col}...")
        r = requests.get(BASE_URL, params={"size": 5000, "offset": 0}, timeout=60)
        r.raise_for_status()
        sample_rows = r.json()
        if isinstance(sample_rows, list):
            for row in sample_rows:
                val = str(row.get(spec_col, ''))
                if 'urol' in val.lower():
                    specialty_val = val
                    print(f"  Found: '{specialty_val}'")
                    break
        if not specialty_val:
            print("  Not found in first 5000 rows — will filter locally")

    # Step 3 — download with filter
    print(f"\n[STEP 3] Downloading urology benchmark records...")
    if specialty_val:
        print(f"  Strategy: server-side filter ({spec_col} = {specialty_val})")
    else:
        print(f"  Strategy: download all, filter locally by keywords")

    urology_rows = []
    offset       = 0
    page         = 1
    total_seen   = 0

    while True:
        print(f"  Page {page:4d} | offset {offset:8,} | ", end="", flush=True)

        if specialty_val:
            rows = fetch_page_filtered(offset, spec_col, specialty_val)
        else:
            rows = fetch_page_filtered(offset, None, None)

        if not rows:
            print("done.")
            break

        total_seen += len(rows)

        if specialty_val:
            # Server already filtered — keep all
            kept = rows
        else:
            # Filter locally
            kept = [r for r in rows if is_urology_row(r, spec_col, desc_col)]

        urology_rows.extend(kept)
        print(f"seen {len(rows):,} | kept {len(kept):,} | urology total: {len(urology_rows):,}")

        if len(rows) < PAGE_SIZE:
            print("  (last page)")
            break

        offset += PAGE_SIZE
        page   += 1
        time.sleep(REQUEST_DELAY)

    if not urology_rows:
        print("  ERROR: 0 urology rows found.")
        sys.exit(1)

    # Step 4 — build DataFrame and calculate benchmark stats
    print(f"\n[STEP 4] Building benchmark statistics...")
    df = pd.DataFrame(urology_rows)
    print(f"  Rows: {len(df):,}  |  Columns: {len(df.columns)}")

    # Show payment columns
    pay_cols = [c for c in df.columns if any(k in c.lower()
                for k in ['pymt', 'chrg', 'alowd', 'amt'])]
    if pay_cols:
        print(f"\n  Payment/charge columns: {pay_cols}")
        for col in pay_cols[:4]:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        print(df[pay_cols[:4]].describe().round(2).to_string())

    # Show geographic breakdown
    if geo_col and geo_col in df.columns:
        print(f"\n  Geographic coverage ({df[geo_col].nunique()} unique areas):")
        print(f"  Sample values: {df[geo_col].value_counts().head(5).to_dict()}")

    # Show top CPT codes by volume
    hcpcs_col = next((c for c in df.columns if 'hcpcs_cd' in c.lower()
                      or c.lower() == 'hcpcs_cd'), None)
    srvcs_col = next((c for c in df.columns if 'srvcs' in c.lower()), None)
    if hcpcs_col and srvcs_col:
        df[srvcs_col] = pd.to_numeric(df[srvcs_col], errors='coerce')
        top_cpt = df.groupby(hcpcs_col)[srvcs_col].sum().sort_values(ascending=False).head(10)
        print(f"\n  Top urology CPT codes by national service volume:")
        for cpt, vol in top_cpt.items():
            desc = ''
            if desc_col:
                d = df[df[hcpcs_col] == cpt][desc_col].iloc[0] if len(df[df[hcpcs_col] == cpt]) else ''
                desc = str(d)[:45]
            print(f"    {cpt:8s}  {vol:10,.0f} services  {desc}")

    # Save
    df.to_csv(OUTPUT_CSV, index=False, encoding='utf-8')
    size_mb = os.path.getsize(OUTPUT_CSV) / 1024 / 1024
    print(f"\n[STEP 5] Saved: {OUTPUT_CSV}  ({size_mb:.2f} MB)")

    # Metadata
    with open(OUTPUT_META, 'w') as f:
        json.dump({
            "dataset"           : "CMS Medicare by Geography and Service 2022",
            "guid"              : CMS_DATASET_GUID,
            "rows_saved"        : len(df),
            "geo_areas"         : int(df[geo_col].nunique()) if geo_col and geo_col in df.columns else 0,
            "download_timestamp": datetime.now().isoformat(),
        }, f, indent=4)

    append_audit_log(
        '05_download_benchmarks.py',
        'CMS Medicare by Geography and Service 2022',
        total_seen, len(df), 'SUCCESS',
        f"{len(df):,} urology benchmark rows across geographic areas"
    )

    print()
    print("=" * 62)
    print("  SCRIPT 05 COMPLETE")
    print(f"  {len(df):,} national benchmark rows saved")
    print("  All 5 data sources downloaded successfully!")
    print()
    print("  DATA COLLECTION SUMMARY:")
    print("  Script 01 — 162,360 rows  — Physician billing (CMS)")
    print("  Script 02 —  13,983 rows  — Inpatient DRGs (CMS)")
    print("  Script 03 —   2,696 rows  — Conditions survey (MEPS)")
    print("  Script 04 —   5,426 rows  — Hospital quality (CMS)")
    print("  Script 05 —  ?????? rows  — Geographic benchmarks (CMS)")
    print()
    print("  Next: python scripts/05_clean_and_validate.py")
    print("=" * 62)


if __name__ == "__main__":
    main()
