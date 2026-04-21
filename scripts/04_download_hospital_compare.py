"""
============================================================
Script 04 — Download CMS Hospital Compare Data
============================================================
PURPOSE:
    Downloads the CMS Hospital General Information dataset
    (Hospital Compare program) for all US hospitals.

    This gives us operational benchmarks:
      - Hospital overall quality rating (1–5 stars)
      - Mortality performance vs national average
      - Safety performance vs national average
      - Readmission performance vs national average
      - Patient experience scores
      - Hospital type and ownership
      - Whether emergency services are available

    In the analytics project, this data lets us:
      - Benchmark Johns Hopkins Urology against similar
        academic medical centers nationally
      - Build the scorecard in Module 4 (National Benchmarking)
      - Provide context for the executive memo

DATA SOURCE:
    CMS Hospital General Information (Hospital Compare)
    Updated regularly — we get the latest snapshot
    API: https://data.cms.gov/provider-data/api/1/datastore/
         query/xubh-q36u/0

HOW TO RUN:
    python scripts/04_download_hospital_compare.py

OUTPUT:
    data/raw/cms_hospital/hospital_compare_latest.csv
    data/raw/cms_hospital/hospital_compare_metadata.json
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
# CMS Provider Data Catalog API — Hospital General Information
# Dataset ID: xubh-q36u (stable identifier for Hospital Compare)
CMS_HOSPITAL_API = "https://data.cms.gov/provider-data/api/1/datastore/query/xubh-q36u/0"
PAGE_SIZE        = 500     # This API uses smaller pages
REQUEST_DELAY    = 0.3

OUTPUT_DIR  = os.path.join(PROJECT_ROOT, 'data', 'raw', 'cms_hospital')
OUTPUT_CSV  = os.path.join(OUTPUT_DIR, 'hospital_compare_latest.csv')
OUTPUT_META = os.path.join(OUTPUT_DIR, 'hospital_compare_metadata.json')

# Academic Medical Centers and top urology programs we want
# to specifically flag in the data (for benchmarking)
TOP_UROLOGY_PROGRAMS = [
    'johns hopkins',
    'mayo clinic',
    'cleveland clinic',
    'memorial sloan',
    'ucsf',
    'university of michigan',
    'vanderbilt',
    'stanford',
    'columbia',
    'cornell',
    'northwestern',
    'brigham',
    'mass general',
]

# ── Helpers ───────────────────────────────────────────────

def fetch_page(offset):
    """
    Download one page of hospital data.
    This API uses a different format than the CMS data API:
    POST request with JSON body specifying limit and offset.
    """
    params = {
        "limit" : PAGE_SIZE,
        "offset": offset,
    }
    for attempt in range(1, 4):
        try:
            r = requests.get(CMS_HOSPITAL_API, params=params, timeout=60)
            r.raise_for_status()
            data = r.json()

            # API returns {"results": [...], "count": N, "total": N}
            if isinstance(data, dict):
                results = data.get('results', data.get('data', []))
                total   = data.get('total', data.get('count', -1))
                return results, total
            elif isinstance(data, list):
                return data, -1
            return [], -1

        except Exception as e:
            if attempt == 3:
                print(f"  ERROR after 3 retries at offset {offset}: {e}")
                return [], -1
            print(f"  Retry {attempt}/3 — {e}")
            time.sleep(3 * attempt)
    return [], -1


def flag_top_programs(df):
    """
    Add a column flagging top urology/academic medical centers
    so we can easily filter them for benchmarking comparisons.
    """
    if 'facility_name' not in df.columns:
        return df
    name_lower = df['facility_name'].str.lower().fillna('')
    df['is_top_urology_program'] = name_lower.apply(
        lambda n: any(prog in n for prog in TOP_UROLOGY_PROGRAMS)
    )
    flagged = df['is_top_urology_program'].sum()
    print(f"  Flagged {flagged} top academic/urology programs")
    return df


# ── Main ──────────────────────────────────────────────────

def main():
    print("=" * 62)
    print("  SCRIPT 04 — CMS Hospital Compare Download")
    print("=" * 62)
    print(f"  Started : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Source  : CMS Hospital General Information (Hospital Compare)")
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

    # Step 1 — get first page to learn total count and columns
    print("[STEP 1] Getting record count and column names...")
    first_page, total = fetch_page(0)

    if not first_page:
        print("  ERROR: No data returned from API.")
        sys.exit(1)

    print(f"  Total hospitals in dataset : {total:,}" if total > 0 else "  Total: unknown")
    print(f"  Columns ({len(first_page[0].keys())}): {list(first_page[0].keys())}")

    # Step 2 — paginate through all hospitals
    print(f"\n[STEP 2] Downloading all hospital records...")
    all_records = list(first_page)
    offset = PAGE_SIZE
    page   = 2

    while True:
        if total > 0 and offset >= total:
            break

        print(f"  Page {page:3d} | offset {offset:6,} | running total: {len(all_records):,}", end="\r")
        rows, _ = fetch_page(offset)

        if not rows:
            break

        all_records.extend(rows)

        if len(rows) < PAGE_SIZE:
            break

        offset += PAGE_SIZE
        page   += 1
        time.sleep(REQUEST_DELAY)

    print(f"\n  Download complete: {len(all_records):,} hospitals")

    # Step 3 — build DataFrame and enrich
    print(f"\n[STEP 3] Processing and enriching data...")
    df = pd.DataFrame(all_records)
    print(f"  Shape: {len(df):,} rows × {len(df.columns)} columns")

    # Convert rating to numeric
    if 'hospital_overall_rating' in df.columns:
        df['hospital_overall_rating'] = pd.to_numeric(
            df['hospital_overall_rating'], errors='coerce'
        )
        rated = df['hospital_overall_rating'].notna().sum()
        avg   = df['hospital_overall_rating'].mean()
        print(f"  Hospitals with star rating: {rated:,}  |  National avg: {avg:.2f} stars")

    # Flag top programs
    df = flag_top_programs(df)

    # Show top urology programs found
    if 'is_top_urology_program' in df.columns and 'facility_name' in df.columns:
        top = df[df['is_top_urology_program']][['facility_name','state','hospital_overall_rating']].head(15)
        if len(top):
            print(f"\n  Top urology/academic programs found:")
            print(top.to_string(index=False))

    # State distribution
    if 'state' in df.columns:
        print(f"\n  Hospitals by state (top 10):")
        top_states = df['state'].value_counts().head(10)
        for state, cnt in top_states.items():
            bar = '█' * (cnt // 50)
            print(f"    {state:4s}  {cnt:4,}  {bar}")

    # Step 4 — save
    df.to_csv(OUTPUT_CSV, index=False, encoding='utf-8')
    size_mb = os.path.getsize(OUTPUT_CSV) / 1024 / 1024
    print(f"\n[STEP 4] Saved: {OUTPUT_CSV}  ({size_mb:.2f} MB)")

    # Metadata
    with open(OUTPUT_META, 'w') as f:
        json.dump({
            "source"            : "CMS Hospital General Information (Hospital Compare)",
            "api_endpoint"      : CMS_HOSPITAL_API,
            "hospitals_total"   : len(df),
            "columns"           : list(df.columns),
            "top_programs_count": int(df['is_top_urology_program'].sum()) if 'is_top_urology_program' in df.columns else 0,
            "download_timestamp": datetime.now().isoformat(),
        }, f, indent=4)

    # Audit
    append_audit_log(
        '04_download_hospital_compare.py',
        'CMS Hospital General Information (Hospital Compare)',
        len(all_records), len(df), 'SUCCESS',
        f"{len(df):,} hospitals, {df['state'].nunique() if 'state' in df.columns else '?'} states"
    )

    print()
    print("=" * 62)
    print("  SCRIPT 04 COMPLETE")
    print(f"  {len(df):,} hospitals saved with quality ratings")
    print("  Next: python scripts/05_download_benchmarks.py")
    print("=" * 62)


if __name__ == "__main__":
    main()
