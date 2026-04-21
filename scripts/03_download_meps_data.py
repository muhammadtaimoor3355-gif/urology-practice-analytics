"""
============================================================
Script 03 — Download MEPS Ambulatory Care Data (Urology)
============================================================
PURPOSE:
    Downloads the AHRQ Medical Expenditure Panel Survey (MEPS)
    2022 Medical Conditions file (H241) and filters to
    urology-related conditions.

    MEPS is a national survey of US families and individuals,
    their medical providers, and employers. It tells us:
      - How many Americans visited a doctor for urology conditions
      - What conditions they were treated for (ICD-10 codes)
      - Patient demographics and insurance (payer mix)
      - Out-of-pocket costs and total expenditures
    This feeds the Capacity & Access analysis module.

DATA SOURCE:
    AHRQ MEPS HC-241: 2022 Medical Conditions File
    URL: https://meps.ahrq.gov/data_stats/download_data/pufs/h241/

UROLOGY ICD-10 CODES WE FILTER FOR:
    N10-N16  Kidney/tubulo-interstitial diseases
    N17-N19  Acute & chronic kidney failure
    N20-N23  Urolithiasis (kidney stones)
    N30-N39  Bladder conditions, UTIs
    N40-N53  Male genital diseases (BPH, prostate)
    C61      Prostate cancer
    C64-C68  Urinary tract cancers
    Z87.39   Personal history of urinary conditions

HOW TO RUN:
    python scripts/03_download_meps_data.py

OUTPUT:
    data/raw/meps/meps_urology_conditions_2022.csv
    data/raw/meps/meps_metadata.json
============================================================
"""

import os
import sys
import io
import json
import zipfile
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
# MEPS H241 = 2022 Medical Conditions file
# AHRQ follows a consistent URL pattern for all MEPS files
MEPS_FILE_CODE = "h241"
MEPS_YEAR      = 2022

# Try Excel first (easiest to parse), then CSV
MEPS_URLS = [
    f"https://meps.ahrq.gov/data_files/pufs/{MEPS_FILE_CODE}/{MEPS_FILE_CODE}xlsx.zip",
    f"https://meps.ahrq.gov/data_files/pufs/{MEPS_FILE_CODE}/{MEPS_FILE_CODE}dat.zip",
]

OUTPUT_DIR  = os.path.join(PROJECT_ROOT, 'data', 'raw', 'meps')
OUTPUT_CSV  = os.path.join(OUTPUT_DIR, f'meps_urology_conditions_{MEPS_YEAR}.csv')
OUTPUT_META = os.path.join(OUTPUT_DIR, 'meps_metadata.json')
RAW_ZIP     = os.path.join(OUTPUT_DIR, f'{MEPS_FILE_CODE}.zip')

# Urology ICD-10 chapter prefixes (first 3 characters of code)
UROLOGY_ICD10_PREFIXES = [
    'N10', 'N11', 'N12', 'N13', 'N14', 'N15', 'N16',  # Kidney diseases
    'N17', 'N18', 'N19',                                 # Kidney failure
    'N20', 'N21', 'N22', 'N23',                          # Kidney stones
    'N25', 'N26', 'N27', 'N28', 'N29',                   # Other kidney disorders
    'N30', 'N31', 'N32', 'N33', 'N34', 'N35', 'N36',    # Bladder conditions
    'N37', 'N38', 'N39',                                  # UTIs
    'N40', 'N41', 'N42', 'N43', 'N44', 'N45', 'N46',    # Male genital (BPH, prostate)
    'N47', 'N48', 'N49', 'N50', 'N51', 'N52', 'N53',
    'C61',                                                # Prostate cancer
    'C64', 'C65', 'C66', 'C67', 'C68',                   # Urinary tract cancers
    'D09', 'D30', 'D41',                                  # In-situ / benign urology tumors
    'R30', 'R31', 'R32', 'R33', 'R34', 'R35', 'R36',    # Symptoms: dysuria, hematuria
    'R80', 'R82',                                         # Proteinuria, other urine findings
]

# Column name possibilities for ICD-10 code in MEPS
ICD10_COL_CANDIDATES = ['ICD10CDX', 'CONDDXSJ', 'ICD10', 'ICDCODE', 'icd10cdx']

# ── Helpers ───────────────────────────────────────────────

def download_zip(url, dest_path):
    """Download a ZIP file with progress display."""
    print(f"  Downloading: {url}")
    try:
        r = requests.get(url, stream=True, timeout=120)
        r.raise_for_status()
        total = int(r.headers.get('content-length', 0))
        downloaded = 0
        with open(dest_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=65536):
                f.write(chunk)
                downloaded += len(chunk)
                if total:
                    pct = downloaded / total * 100
                    print(f"\r  Progress: {pct:.0f}% ({downloaded/1024/1024:.1f} MB)", end='', flush=True)
        print(f"\n  Download complete: {os.path.getsize(dest_path)/1024/1024:.1f} MB")
        return True
    except requests.exceptions.HTTPError as e:
        print(f"\n  HTTP Error: {e}")
        return False
    except requests.exceptions.ConnectionError:
        print("\n  Connection error — check internet.")
        return False
    except Exception as e:
        print(f"\n  Error: {e}")
        return False


def extract_and_read_zip(zip_path):
    """
    Extract ZIP and read the data file inside.
    MEPS ZIPs contain one Excel (.xlsx) or ASCII (.dat) file.
    Returns a DataFrame or None.
    """
    print(f"\n[STEP 3] Extracting and reading ZIP contents...")
    try:
        with zipfile.ZipFile(zip_path, 'r') as z:
            names = z.namelist()
            print(f"  Files inside ZIP: {names}")

            # Try Excel first
            xlsx_files = [n for n in names if n.lower().endswith('.xlsx')]
            csv_files  = [n for n in names if n.lower().endswith('.csv')]
            dat_files  = [n for n in names if n.lower().endswith('.dat') or n.lower().endswith('.txt')]

            if xlsx_files:
                print(f"  Reading Excel file: {xlsx_files[0]}")
                with z.open(xlsx_files[0]) as f:
                    df = pd.read_excel(io.BytesIO(f.read()), engine='openpyxl')
                print(f"  Rows: {len(df):,}  |  Columns: {len(df.columns)}")
                return df

            elif csv_files:
                print(f"  Reading CSV file: {csv_files[0]}")
                with z.open(csv_files[0]) as f:
                    df = pd.read_csv(f, low_memory=False)
                print(f"  Rows: {len(df):,}  |  Columns: {len(df.columns)}")
                return df

            elif dat_files:
                # ASCII fixed-width file — needs column positions from codebook
                # Read as text and attempt comma-separated parse first
                print(f"  Found ASCII file: {dat_files[0]} — attempting CSV parse...")
                with z.open(dat_files[0]) as f:
                    content = f.read().decode('latin-1', errors='replace')
                lines = content.split('\n')
                print(f"  First line sample: {lines[0][:120]}")
                # Try comma-separated
                try:
                    df = pd.read_csv(io.StringIO(content), sep=',', low_memory=False)
                    print(f"  Parsed as CSV: {len(df):,} rows")
                    return df
                except Exception:
                    # Try space-separated
                    df = pd.read_csv(io.StringIO(content), sep=r'\s+',
                                     low_memory=False, on_bad_lines='skip')
                    print(f"  Parsed as space-delimited: {len(df):,} rows")
                    return df
            else:
                print(f"  ERROR: No readable file found in ZIP. Contents: {names}")
                return None

    except zipfile.BadZipFile:
        print("  ERROR: Downloaded file is not a valid ZIP.")
        return None
    except Exception as e:
        print(f"  ERROR reading ZIP: {e}")
        return None


def find_icd10_column(df):
    """Find whichever column holds ICD-10 condition codes."""
    for candidate in ICD10_COL_CANDIDATES:
        if candidate in df.columns:
            return candidate
        if candidate.lower() in [c.lower() for c in df.columns]:
            # Case-insensitive match
            return next(c for c in df.columns if c.lower() == candidate.lower())
    # Last resort — look for any column with sample values like 'N40', 'C61'
    for col in df.columns:
        sample = df[col].dropna().astype(str).head(100)
        hits = sample.str.match(r'^[A-Z]\d{2}').sum()
        if hits > 10:
            print(f"  Auto-detected ICD-10 column: '{col}'")
            return col
    return None


def filter_to_urology(df, icd10_col):
    """Keep only rows where ICD-10 code starts with a urology prefix."""
    df[icd10_col] = df[icd10_col].astype(str).str.strip().str.upper()
    mask = df[icd10_col].str[:3].isin(UROLOGY_ICD10_PREFIXES)
    return df[mask].copy()


# ── Main ──────────────────────────────────────────────────

def main():
    print("=" * 62)
    print("  SCRIPT 03 — MEPS Ambulatory Urology Data Download")
    print("=" * 62)
    print(f"  Started  : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  File     : MEPS H241 — 2022 Medical Conditions")
    print(f"  Filtering: {len(UROLOGY_ICD10_PREFIXES)} urology ICD-10 prefixes")
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

    # Step 1 — download ZIP (try Excel URL first, then ASCII)
    print("[STEP 1] Downloading MEPS H241 ZIP file...")
    downloaded = False
    used_url   = None
    for url in MEPS_URLS:
        if download_zip(url, RAW_ZIP):
            downloaded = True
            used_url   = url
            break

    if not downloaded:
        print("\n  All download URLs failed.")
        print("  Manual fallback instructions:")
        print("  1. Go to: https://meps.ahrq.gov/mepsweb/data_stats/download_data_files.jsp")
        print("  2. Search for HC-241 (2022 Medical Conditions)")
        print("  3. Download the Excel or CSV version")
        print(f"  4. Save to: {OUTPUT_DIR}")
        print("  5. Re-run this script")
        append_audit_log('03_download_meps_data.py', 'MEPS H241 2022',
                         0, 0, 'ERROR', 'All download URLs failed')
        sys.exit(1)

    # Step 2 — verify ZIP is valid
    print(f"\n[STEP 2] Verifying downloaded file...")
    size_mb = os.path.getsize(RAW_ZIP) / 1024 / 1024
    print(f"  Size: {size_mb:.1f} MB")
    if size_mb < 0.1:
        print("  ERROR: File is too small — download may have failed.")
        sys.exit(1)

    # Step 3 — extract and read
    df_raw = extract_and_read_zip(RAW_ZIP)
    if df_raw is None:
        sys.exit(1)

    print(f"\n  All columns: {list(df_raw.columns[:20])}{'...' if len(df_raw.columns)>20 else ''}")

    # Step 4 — find ICD-10 column and filter
    print(f"\n[STEP 4] Finding ICD-10 column and filtering to urology...")
    icd10_col = find_icd10_column(df_raw)

    if not icd10_col:
        print("  WARNING: Could not find ICD-10 column.")
        print("  Saving full dataset for manual review.")
        df_urology = df_raw
    else:
        print(f"  ICD-10 column: '{icd10_col}'")
        print(f"  Sample values: {df_raw[icd10_col].dropna().head(10).tolist()}")
        df_urology = filter_to_urology(df_raw, icd10_col)
        print(f"  Urology rows: {len(df_urology):,} of {len(df_raw):,} total")

    if len(df_urology) == 0:
        print("  WARNING: 0 urology rows found — saving full file for inspection.")
        df_urology = df_raw

    # Step 5 — save
    df_urology.to_csv(OUTPUT_CSV, index=False, encoding='utf-8')
    size_out = os.path.getsize(OUTPUT_CSV) / 1024 / 1024
    print(f"\n[STEP 5] Saved: {OUTPUT_CSV}  ({size_out:.2f} MB)")

    # Top conditions preview
    if icd10_col and icd10_col in df_urology.columns:
        print(f"\n  Top urology conditions by ICD-10 code:")
        top = df_urology[icd10_col].value_counts().head(12)
        for code, cnt in top.items():
            print(f"    {code:8s}  {cnt:6,} records")

    # Metadata
    with open(OUTPUT_META, 'w') as f:
        json.dump({
            "meps_file"         : "H241",
            "data_year"         : MEPS_YEAR,
            "source_url"        : used_url,
            "rows_total"        : len(df_raw),
            "rows_urology"      : len(df_urology),
            "icd10_column_used" : icd10_col,
            "download_timestamp": datetime.now().isoformat(),
        }, f, indent=4)

    # Audit
    append_audit_log(
        '03_download_meps_data.py', f'MEPS H241 2022 Medical Conditions',
        len(df_raw), len(df_urology), 'SUCCESS',
        f"ICD-10 column: {icd10_col}, {len(df_urology):,} urology condition records"
    )

    print()
    print("=" * 62)
    print("  SCRIPT 03 COMPLETE")
    print(f"  {len(df_urology):,} urology condition records saved")
    print("  Next: python scripts/04_download_hospital_compare.py")
    print("=" * 62)


if __name__ == "__main__":
    main()
