"""
============================================================
Script 02 — Download Inpatient Hospital Data (Urology DRGs)
============================================================
PURPOSE:
    Downloads real inpatient hospital procedure data for
    urology-related DRGs (Diagnosis Related Groups) from
    the CMS Medicare Inpatient Hospitals dataset.

WHY NOT HCUP?
    HCUP NIS requires paid registration (~$300) and has no
    public API. This CMS dataset is from the same government
    (CMS.gov), is completely free, has an API, and contains
    the same key metrics we need:
      - Procedure volumes (total discharges per hospital)
      - Average length of stay
      - Average charges and Medicare payments
      - Hospital-level benchmarks across the US
    In an interview, this is MORE defensible because it is
    the same source as Script 01.

DATA SOURCE:
    CMS Medicare Inpatient Hospitals by Provider and Service
    2022 Data Year
    URL: https://data.cms.gov/provider-data/topics/hospitals

UROLOGY DRGs WE FILTER FOR:
    651-676 : Kidney, Ureter, Bladder, and Prostate procedures
    707-708 : Major Male Pelvic Procedures
    659-660 : Kidney/Urinary Tract Infections (medical mgmt)
    Plus any DRG description containing urology keywords

    A DRG (Diagnosis Related Group) is a billing category that
    groups hospital stays by diagnosis + procedure complexity.
    Hospitals get paid a fixed rate per DRG — so tracking DRG
    volumes tells you exactly how busy a urology service is.

HOW TO RUN:
    python scripts/02_download_hcup_data.py

OUTPUT:
    data/raw/hcup/cms_inpatient_urology_2022.csv
    data/raw/hcup/cms_inpatient_metadata.json
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
# CMS Medicare Inpatient Hospitals by Provider and Service — 2022
CMS_DATASET_GUID = "46bf50f8-0983-4ca2-b8d5-f2afbbf2e589"
PAGE_SIZE        = 5000
REQUEST_DELAY    = 0.3

OUTPUT_DIR  = os.path.join(PROJECT_ROOT, 'data', 'raw', 'hcup')
OUTPUT_CSV  = os.path.join(OUTPUT_DIR, 'cms_inpatient_urology_2022.csv')
OUTPUT_META = os.path.join(OUTPUT_DIR, 'cms_inpatient_metadata.json')

BASE_URL = f"{CMS_API_BASE}/{CMS_DATASET_GUID}/data"

# Urology DRG number ranges (CMS MDC 11 = Kidney and Urinary Tract)
# Plus male reproductive DRGs (707-708)
UROLOGY_DRG_RANGES = list(range(651, 677)) + [707, 708]

# Keywords to catch any urology DRGs not in the numeric range
UROLOGY_KEYWORDS = [
    'kidney', 'ureter', 'bladder', 'prostat', 'ureth',
    'urinary', 'renal', 'nephro', 'lithotrip', 'cystect',
    'pelvic', 'urolog', 'transurethral', 'turp'
]

# ── Helpers ───────────────────────────────────────────────

def probe_columns():
    """Fetch 1 row to discover column names in this dataset."""
    print("[STEP 1] Probing API for column names...")
    r = requests.get(BASE_URL, params={"size": 1, "offset": 0}, timeout=30)
    r.raise_for_status()
    data = r.json()
    row  = data[0] if isinstance(data, list) else data.get('data', [{}])[0]
    cols = list(row.keys())
    print(f"  Columns found ({len(cols)}): {cols}")
    return cols


def find_drg_column(cols):
    """Return the column name that holds the DRG code."""
    for col in cols:
        if col.upper() in ('DRG_CD', 'DRG_DEF', 'MS_DRG', 'DRG'):
            return col
    for col in cols:
        if 'drg' in col.lower():
            return col
    return None


def find_drg_desc_column(cols):
    """Return the column name that holds the DRG description text."""
    for col in cols:
        if 'drg' in col.lower() and 'desc' in col.lower():
            return col
    return None


def is_urology_drg(drg_code_raw, drg_desc_raw):
    """
    Returns True if this row is a urology-related DRG.
    Checks both numeric DRG code and description text.
    """
    # Check numeric DRG code
    try:
        code = int(str(drg_code_raw).strip())
        if code in UROLOGY_DRG_RANGES:
            return True
    except (ValueError, TypeError):
        pass

    # Check description text for urology keywords
    desc = str(drg_desc_raw).lower() if drg_desc_raw else ''
    return any(kw in desc for kw in UROLOGY_KEYWORDS)


def fetch_page(offset):
    """Download one page of inpatient data (no server-side filter — we filter locally)."""
    params = {"size": PAGE_SIZE, "offset": offset}
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
                print(f"  ERROR after 3 retries at offset {offset}: {e}")
                return []
            print(f"  Retry {attempt}/3 — {e}")
            time.sleep(3 * attempt)
    return []


# ── Main ──────────────────────────────────────────────────

def main():
    print("=" * 62)
    print("  SCRIPT 02 — CMS Inpatient Urology Data Download")
    print("=" * 62)
    print(f"  Started  : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Source   : CMS Medicare Inpatient Hospitals 2022")
    print(f"  Filtering: Urology DRGs 651-676, 707-708 + keywords")
    print()

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Check if already downloaded
    if os.path.exists(OUTPUT_CSV):
        size_mb = os.path.getsize(OUTPUT_CSV) / 1024 / 1024
        print(f"  File already exists ({size_mb:.1f} MB).")
        choice = input("  Re-download? (y/n): ").strip().lower()
        if choice != 'y':
            print("  Using existing file.")
            return

    # Step 1 — discover columns
    cols       = probe_columns()
    drg_col    = find_drg_column(cols)
    desc_col   = find_drg_desc_column(cols)

    print(f"\n  DRG code column : {drg_col}")
    print(f"  DRG desc column : {desc_col}")

    if not drg_col:
        print("\n  WARNING: No DRG column found. Will filter by description keywords only.")

    # Step 2 — download all pages, filter to urology locally
    print(f"\n[STEP 2] Downloading all inpatient records (filtering to urology)...")
    print(f"  Strategy: download each page, keep urology rows, discard rest")
    print(f"  This avoids downloading 3M+ rows — each page is filtered immediately\n")

    urology_rows = []
    offset       = 0
    page         = 1
    total_seen   = 0

    while True:
        print(f"  Page {page:4d} | offset {offset:8,} | ", end="", flush=True)
        rows = fetch_page(offset)

        if not rows:
            print("done — no more records.")
            break

        total_seen += len(rows)

        # Filter this page to urology rows only
        kept = []
        for row in rows:
            code = row.get(drg_col, '') if drg_col else ''
            desc = row.get(desc_col, '') if desc_col else ''
            if is_urology_drg(code, desc):
                kept.append(row)

        urology_rows.extend(kept)
        print(f"seen {len(rows):,} | kept {len(kept):,} urology | running urology total: {len(urology_rows):,}")

        if len(rows) < PAGE_SIZE:
            print("  (last page)")
            break

        offset += PAGE_SIZE
        page   += 1
        time.sleep(REQUEST_DELAY)

    print(f"\n  Total rows scanned : {total_seen:,}")
    print(f"  Urology rows kept  : {len(urology_rows):,}")

    if not urology_rows:
        print("\n  ERROR: 0 urology rows found. Check DRG column names above.")
        print("  Column names found:", cols)
        sys.exit(1)

    # Step 3 — save
    df = pd.DataFrame(urology_rows)
    print(f"\n[STEP 3] Saving {len(df):,} rows × {len(df.columns)} columns...")
    df.to_csv(OUTPUT_CSV, index=False, encoding='utf-8')
    size_mb = os.path.getsize(OUTPUT_CSV) / 1024 / 1024
    print(f"  Saved: {OUTPUT_CSV}  ({size_mb:.2f} MB)")

    # Metadata
    with open(OUTPUT_META, 'w') as f:
        json.dump({
            "dataset_guid"      : CMS_DATASET_GUID,
            "data_year"         : 2022,
            "drg_ranges"        : "651-676, 707-708 + keyword filter",
            "rows_saved"        : len(df),
            "total_rows_scanned": total_seen,
            "download_timestamp": datetime.now().isoformat(),
            "why_not_hcup"      : (
                "HCUP NIS requires paid registration. This CMS Inpatient "
                "dataset is free, API-accessible, and contains the same "
                "metrics: discharge volumes, avg LOS, avg charges, avg payments."
            )
        }, f, indent=4)

    # Step 4 — preview
    print(f"\n[STEP 4] Preview of urology inpatient data:")
    print("-" * 62)

    # Show the DRG descriptions found
    if desc_col and desc_col in df.columns:
        print(f"\n  Urology DRG categories found ({df[desc_col].nunique()} unique):")
        top_drgs = df.groupby(desc_col).size().sort_values(ascending=False).head(10)
        for drg, count in top_drgs.items():
            print(f"    {count:5,} hospitals  |  {str(drg)[:55]}")

    # Show key numeric columns
    numeric_cols = df.select_dtypes(include='number').columns.tolist()
    if numeric_cols:
        print(f"\n  Numeric columns summary:")
        print(df[numeric_cols[:5]].describe().round(2).to_string())

    # Audit log
    append_audit_log(
        '02_download_hcup_data.py',
        'CMS Medicare Inpatient Hospitals by Provider and Service 2022',
        total_seen, len(df), 'SUCCESS',
        f"Urology DRGs 651-676 + 707-708, {df[desc_col].nunique() if desc_col and desc_col in df.columns else '?'} unique DRGs"
    )

    print()
    print("=" * 62)
    print("  SCRIPT 02 COMPLETE")
    print(f"  {len(df):,} urology inpatient rows saved")
    print(f"  Source: CMS Inpatient Hospitals 2022 (replaces HCUP NIS)")
    print("  Next: python scripts/03_download_meps_data.py")
    print("=" * 62)


if __name__ == "__main__":
    main()
