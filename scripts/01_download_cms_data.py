"""
============================================================
Script 01 — Download CMS Medicare Physician Data (Urology)
============================================================
PURPOSE:
    Downloads real Medicare billing data for ALL urologists
    in the US from the CMS public API.
    Filters to Urology specialty ONLY — ~15,000 rows.
    Should complete in under 2 minutes.

DATA SOURCE:
    CMS Medicare Physician & Other Practitioners
    by Provider and Service — 2022 Data Year
    URL: https://data.cms.gov/provider-summary-by-type-of-service/
         medicare-physician-other-practitioners/
         medicare-physician-other-practitioners-by-provider-and-service

HOW TO RUN:
    python scripts/01_download_cms_data.py

OUTPUT:
    data/raw/cms_physician/cms_urology_2022.csv
============================================================
"""

import os
import sys
import json
import time
import requests
import pandas as pd
from datetime import datetime

# ── Project root ──────────────────────────────────
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.join(PROJECT_ROOT, 'scripts'))

try:
    from _00_setup_environment import append_audit_log
except ImportError:
    def append_audit_log(*a, **k): pass

# ── Config ───────────────────────────────────────
CMS_API_BASE     = "https://data.cms.gov/data-api/v1/dataset"
CMS_DATASET_GUID = "0e9f2f2b-7bf9-451a-912c-e02e654dd725"
PAGE_SIZE        = 5000
REQUEST_DELAY    = 0.3

OUTPUT_DIR  = os.path.join(PROJECT_ROOT, 'data', 'raw', 'cms_physician')
OUTPUT_CSV  = os.path.join(OUTPUT_DIR, 'cms_urology_2022.csv')
OUTPUT_META = os.path.join(OUTPUT_DIR, 'cms_api_metadata.json')

BASE_URL = f"{CMS_API_BASE}/{CMS_DATASET_GUID}/data"

# ── Helpers ──────────────────────────────────────

def find_specialty_column(df):
    """Return the name of whatever column holds the provider specialty."""
    # CMS uses 'Rndrng_Prvdr_Type' in the by-provider-and-service dataset
    for col in df.columns:
        if col == 'Rndrng_Prvdr_Type':
            return col
    for col in df.columns:
        if any(k in col.lower() for k in ['spclty', 'specialty', 'type', 'prvdr_type']):
            return col
    return None


def fetch_page(offset, specialty_col, specialty_value):
    """
    Download one page of records, filtered server-side to Urology.
    Uses requests' params dict so brackets are URL-encoded correctly —
    that was the bug that caused 3M rows to download before.
    """
    # Pass filter as a dict — requests encodes filter[Col]=Val properly
    params = {
        f"filter[{specialty_col}]": specialty_value,
        "size": PAGE_SIZE,
        "offset": offset,
    }

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


def probe_column_name():
    """
    Download 1 row (no filter) to discover the exact column name
    for provider specialty in this dataset version.
    """
    print("[STEP 1] Probing API to find specialty column name...")
    r = requests.get(BASE_URL, params={"size": 1, "offset": 0}, timeout=30)
    r.raise_for_status()
    data = r.json()
    row = data[0] if isinstance(data, list) else data.get('data', [{}])[0]
    df  = pd.DataFrame([row])
    col = find_specialty_column(df)
    if col:
        print(f"  Specialty column found: '{col}'")
    else:
        print(f"  WARNING: could not find specialty column. Columns: {list(df.columns)}")
    return col


def probe_specialty_value(specialty_col):
    """
    Download a few rows filtered loosely to find the exact string CMS
    uses for Urology (could be 'Urology', 'UROLOGY', etc.).
    We grab the first page unfiltered and look for any Urology-like value.
    """
    print("[STEP 2] Checking exact specialty label used by CMS...")
    # Download first 5000 rows and scan for urology label
    r = requests.get(BASE_URL,
                     params={"size": 5000, "offset": 0},
                     timeout=60)
    r.raise_for_status()
    data = r.json()
    rows = data if isinstance(data, list) else data.get('data', [])
    df   = pd.DataFrame(rows)
    if specialty_col not in df.columns:
        return "Urology"  # fallback
    matches = df[specialty_col].dropna().unique()
    for val in matches:
        if 'urol' in str(val).lower():
            print(f"  Found urology label: '{val}'")
            return val
    # If not in first 5000 rows, fall back to standard label
    print("  Not found in first page — using default 'Urology'")
    return "Urology"


# ── Main ─────────────────────────────────────────

def main():
    print("=" * 60)
    print("  SCRIPT 01 — CMS Urology Data Download (FIXED)")
    print("=" * 60)
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # If good data already exists, skip download
    if os.path.exists(OUTPUT_CSV):
        try:
            existing = pd.read_csv(OUTPUT_CSV, nrows=5, low_memory=False)
            spec_col = find_specialty_column(existing)
            if spec_col and 'urol' in str(existing[spec_col].iloc[0]).lower():
                size_mb = os.path.getsize(OUTPUT_CSV) / 1024 / 1024
                print(f"  Good urology data already on disk ({size_mb:.1f} MB).")
                choice = input("  Re-download? (y/n): ").strip().lower()
                if choice != 'y':
                    print("  Using existing file. Move to next script.")
                    return
        except Exception:
            pass  # file is corrupt or wrong — re-download

    # Step 1 — find specialty column name
    specialty_col = probe_column_name()
    if not specialty_col:
        print("  FATAL: Cannot find specialty column. Exiting.")
        sys.exit(1)

    # Step 2 — find exact value label
    specialty_val = probe_specialty_value(specialty_col)

    # Step 3 — paginate with working filter
    print(f"\n[STEP 3] Downloading all '{specialty_val}' records...")
    print(f"  Filter : {specialty_col} = {specialty_val}")
    print(f"  Each page = {PAGE_SIZE:,} rows | pause = {REQUEST_DELAY}s\n")

    all_records = []
    offset      = 0
    page        = 1

    while True:
        print(f"  Page {page:3d} | offset {offset:7,} | ", end="", flush=True)
        rows = fetch_page(offset, specialty_col, specialty_val)

        if not rows:
            print("done — no more records.")
            break

        all_records.extend(rows)
        print(f"{len(rows):,} rows  →  total {len(all_records):,}")

        if len(rows) < PAGE_SIZE:
            print("  (last page)")
            break

        offset += PAGE_SIZE
        page   += 1
        time.sleep(REQUEST_DELAY)

    if not all_records:
        print("\n  ERROR: 0 records downloaded. Check internet / API.")
        sys.exit(1)

    # Step 4 — save
    df = pd.DataFrame(all_records)
    print(f"\n[STEP 4] Saving {len(df):,} rows × {len(df.columns)} columns...")

    # Safety: keep only urology rows in case any slipped through
    spec_col_df = find_specialty_column(df)
    if spec_col_df:
        before = len(df)
        df = df[df[spec_col_df].str.contains('urol', case=False, na=False)].copy()
        print(f"  Post-filter: {len(df):,} rows (was {before:,})")

    df.to_csv(OUTPUT_CSV, index=False, encoding='utf-8')
    size_mb = os.path.getsize(OUTPUT_CSV) / 1024 / 1024
    print(f"  Saved: {OUTPUT_CSV}  ({size_mb:.2f} MB)")

    # Metadata
    with open(OUTPUT_META, 'w') as f:
        json.dump({
            "dataset_guid"       : CMS_DATASET_GUID,
            "specialty_filter"   : specialty_val,
            "rows_downloaded"    : len(df),
            "download_timestamp" : datetime.now().isoformat(),
        }, f, indent=4)

    # Preview
    print("\n[STEP 5] Quick preview (first 3 rows, key columns):")
    show = [c for c in df.columns if any(k in c.lower()
            for k in ['npi','name','state','hcpcs','srvcs','pymt'])][:6]
    print(df[show].head(3).to_string(index=False) if show else df.head(3).to_string())

    # Audit
    npi_col = next((c for c in df.columns if 'npi' in c.lower()), None)
    unique_docs = df[npi_col].nunique() if npi_col else '?'
    append_audit_log('01_download_cms_data.py',
                     'CMS Medicare Physician API 2022',
                     len(all_records), len(df), 'SUCCESS',
                     f"{unique_docs} unique urologist NPIs")

    print()
    print("=" * 60)
    print("  SCRIPT 01 COMPLETE")
    print(f"  {len(df):,} rows saved  |  {unique_docs} unique urologists")
    print("  Next: python scripts/02_download_hcup_data.py")
    print("=" * 60)


if __name__ == "__main__":
    main()
