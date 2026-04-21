"""
============================================================
Script 06 — Clean and Validate All Raw Datasets
============================================================
PURPOSE:
    Takes all 5 raw downloaded datasets and produces clean,
    analysis-ready CSV files in data/processed/.

    What "cleaning" means here:
      - Rename confusing CMS column codes to readable names
        (e.g. Rndrng_NPI → provider_npi)
      - Convert text numbers to actual numbers ("1,234" → 1234)
      - Remove duplicate rows
      - Flag and report missing values
      - Filter out non-urology rows that slipped through
      - Standardize state abbreviations and text case
      - Add calculated columns needed for analysis
        (e.g. estimated_annual_revenue per physician)

    Each dataset gets its own cleaning section below.
    At the end, a data quality report is printed.

HOW TO RUN:
    python scripts/06_clean_and_validate.py

OUTPUT:
    data/processed/physician_clean.csv
    data/processed/inpatient_clean.csv
    data/processed/meps_clean.csv
    data/processed/hospital_clean.csv
    data/processed/benchmarks_clean.csv
    outputs/reports/data_quality_report.csv
============================================================
"""

import os
import sys
import json
import pandas as pd
import numpy as np
from datetime import datetime

# ── Project root ──────────────────────────────────────────
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.join(PROJECT_ROOT, 'scripts'))

try:
    from _00_setup_environment import append_audit_log
except ImportError:
    def append_audit_log(*a, **k): pass

# ── Paths ─────────────────────────────────────────────────
RAW   = os.path.join(PROJECT_ROOT, 'data', 'raw')
PROC  = os.path.join(PROJECT_ROOT, 'data', 'processed')
RPTS  = os.path.join(PROJECT_ROOT, 'outputs', 'reports')

os.makedirs(PROC, exist_ok=True)
os.makedirs(RPTS, exist_ok=True)

# ── Quality report accumulator ────────────────────────────
quality_log = []

def log_quality(dataset, rows_in, rows_out, issues):
    quality_log.append({
        'dataset'  : dataset,
        'rows_in'  : rows_in,
        'rows_out' : rows_out,
        'dropped'  : rows_in - rows_out,
        'issues'   : ' | '.join(issues) if issues else 'none',
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    })

def to_numeric_safe(series):
    """Convert a column to numeric, removing commas and $ signs first."""
    return pd.to_numeric(
        series.astype(str).str.replace(',', '').str.replace('$', '').str.strip(),
        errors='coerce'
    )

def section(title):
    print(f"\n{'='*62}")
    print(f"  {title}")
    print(f"{'='*62}")

# ── DATASET 1: Physician Billing (Script 01) ──────────────

def clean_physician():
    section("DATASET 1 — Physician Billing (cms_urology_2022.csv)")

    path = os.path.join(RAW, 'cms_physician', 'cms_urology_2022.csv')
    df   = pd.read_csv(path, low_memory=False)
    rows_in = len(df)
    print(f"  Loaded: {rows_in:,} rows × {len(df.columns)} columns")
    issues = []

    # ── Rename CMS codes to readable names ──
    rename = {
        'Rndrng_NPI'              : 'provider_npi',
        'Rndrng_Prvdr_Last_Org_Name': 'last_name',
        'Rndrng_Prvdr_First_Name' : 'first_name',
        'Rndrng_Prvdr_MI'         : 'middle_initial',
        'Rndrng_Prvdr_Crdntls'    : 'credentials',
        'Rndrng_Prvdr_Ent_Cd'     : 'entity_code',
        'Rndrng_Prvdr_St1'        : 'address',
        'Rndrng_Prvdr_City'       : 'city',
        'Rndrng_Prvdr_State_Abrvtn': 'state',
        'Rndrng_Prvdr_Zip5'       : 'zip_code',
        'Rndrng_Prvdr_RUCA'       : 'ruca_code',
        'Rndrng_Prvdr_RUCA_Desc'  : 'ruca_desc',
        'Rndrng_Prvdr_Type'       : 'specialty',
        'HCPCS_Cd'                : 'cpt_code',
        'HCPCS_Desc'              : 'cpt_description',
        'HCPCS_Drug_Ind'          : 'is_drug_code',
        'Place_Of_Srvc'           : 'place_of_service',
        'Tot_Benes'               : 'total_beneficiaries',
        'Tot_Srvcs'               : 'total_services',
        'Tot_Bene_Day_Srvcs'      : 'total_bene_days',
        'Avg_Sbmtd_Chrg'          : 'avg_submitted_charge',
        'Avg_Mdcr_Alowd_Amt'      : 'avg_allowed_amount',
        'Avg_Mdcr_Pymt_Amt'       : 'avg_medicare_payment',
        'Avg_Mdcr_Stdzd_Amt'      : 'avg_standardized_payment',
    }
    df.rename(columns={k: v for k, v in rename.items() if k in df.columns}, inplace=True)

    # ── Convert numeric columns ──
    num_cols = ['total_beneficiaries', 'total_services', 'avg_submitted_charge',
                'avg_allowed_amount', 'avg_medicare_payment', 'avg_standardized_payment']
    for col in num_cols:
        if col in df.columns:
            df[col] = to_numeric_safe(df[col])

    # ── Remove duplicates ──
    before = len(df)
    df.drop_duplicates(inplace=True)
    if len(df) < before:
        issues.append(f"removed {before - len(df)} duplicates")

    # ── Filter: keep only Urology rows ──
    if 'specialty' in df.columns:
        df = df[df['specialty'].str.contains('Urology', case=False, na=False)].copy()

    # ── Add calculated columns ──
    # Estimated annual revenue per physician-CPT combination
    # = total services × avg medicare payment
    if 'total_services' in df.columns and 'avg_medicare_payment' in df.columns:
        df['estimated_annual_revenue'] = (
            df['total_services'] * df['avg_medicare_payment']
        ).round(2)

    # Discount rate: how much less than submitted charge Medicare actually pays
    if 'avg_submitted_charge' in df.columns and 'avg_medicare_payment' in df.columns:
        df['payment_to_charge_ratio'] = (
            df['avg_medicare_payment'] / df['avg_submitted_charge'].replace(0, np.nan)
        ).round(4)

    # ── Missing value check ──
    key_cols = ['provider_npi', 'cpt_code', 'total_services', 'avg_medicare_payment']
    for col in key_cols:
        if col in df.columns:
            n_missing = df[col].isna().sum()
            if n_missing > 0:
                issues.append(f"{col}: {n_missing} missing")

    # ── State standardize ──
    if 'state' in df.columns:
        df['state'] = df['state'].str.upper().str.strip()

    rows_out = len(df)
    out_path = os.path.join(PROC, 'physician_clean.csv')
    df.to_csv(out_path, index=False)

    print(f"  Rows out       : {rows_out:,}")
    print(f"  Unique NPIs    : {df['provider_npi'].nunique():,}" if 'provider_npi' in df.columns else "")
    print(f"  Unique CPT codes: {df['cpt_code'].nunique():,}" if 'cpt_code' in df.columns else "")
    print(f"  Total revenue est.: ${df['estimated_annual_revenue'].sum():,.0f}" if 'estimated_annual_revenue' in df.columns else "")
    print(f"  Issues         : {issues if issues else 'none'}")
    print(f"  Saved          : {out_path}")

    log_quality('physician_billing', rows_in, rows_out, issues)
    return df


# ── DATASET 2: Inpatient DRGs (Script 02) ─────────────────

def clean_inpatient():
    section("DATASET 2 — Inpatient DRGs (cms_inpatient_urology_2022.csv)")

    path = os.path.join(RAW, 'hcup', 'cms_inpatient_urology_2022.csv')
    df   = pd.read_csv(path, low_memory=False)
    rows_in = len(df)
    print(f"  Loaded: {rows_in:,} rows × {len(df.columns)} columns")
    issues = []

    # ── Rename ──
    rename = {
        'Rndrng_Prvdr_CCN'       : 'hospital_ccn',
        'Rndrng_Prvdr_Org_Name'  : 'hospital_name',
        'Rndrng_Prvdr_City'      : 'city',
        'Rndrng_Prvdr_St'        : 'address',
        'Rndrng_Prvdr_State_Abrvtn': 'state',
        'Rndrng_Prvdr_Zip5'      : 'zip_code',
        'DRG_Cd'                 : 'drg_code',
        'DRG_Desc'               : 'drg_description',
        'Tot_Dschrgs'            : 'total_discharges',
        'Avg_Submtd_Cvrd_Chrg'   : 'avg_submitted_charge',
        'Avg_Tot_Pymt_Amt'       : 'avg_total_payment',
        'Avg_Mdcr_Pymt_Amt'      : 'avg_medicare_payment',
    }
    df.rename(columns={k: v for k, v in rename.items() if k in df.columns}, inplace=True)

    # ── Numerics ──
    for col in ['drg_code', 'total_discharges', 'avg_submitted_charge',
                'avg_total_payment', 'avg_medicare_payment']:
        if col in df.columns:
            df[col] = to_numeric_safe(df[col])

    # ── Remove non-urology rows (the cholecystectomy that slipped in) ──
    UROLOGY_DRG_RANGE = list(range(651, 677)) + [707, 708, 673, 674, 675, 676]
    UROLOGY_KEYWORDS  = ['kidney', 'ureter', 'bladder', 'prostat', 'ureth',
                         'urinary', 'renal', 'nephro', 'lithotrip', 'transurethral']
    if 'drg_code' in df.columns and 'drg_description' in df.columns:
        code_mask = df['drg_code'].isin(UROLOGY_DRG_RANGE)
        desc_mask = df['drg_description'].str.lower().str.contains(
            '|'.join(UROLOGY_KEYWORDS), na=False
        )
        before = len(df)
        df = df[code_mask | desc_mask].copy()
        removed = before - len(df)
        if removed > 0:
            issues.append(f"removed {removed} non-urology DRG rows")

    # ── Add: estimated total annual hospital revenue from urology DRG ──
    if 'total_discharges' in df.columns and 'avg_total_payment' in df.columns:
        df['estimated_drg_revenue'] = (
            df['total_discharges'] * df['avg_total_payment']
        ).round(2)

    if 'state' in df.columns:
        df['state'] = df['state'].str.upper().str.strip()

    df.drop_duplicates(inplace=True)

    rows_out = len(df)
    out_path = os.path.join(PROC, 'inpatient_clean.csv')
    df.to_csv(out_path, index=False)

    print(f"  Rows out          : {rows_out:,}")
    print(f"  Unique DRG codes  : {df['drg_code'].nunique():,}" if 'drg_code' in df.columns else "")
    print(f"  Unique hospitals  : {df['hospital_ccn'].nunique():,}" if 'hospital_ccn' in df.columns else "")
    print(f"  Issues            : {issues if issues else 'none'}")
    print(f"  Saved             : {out_path}")

    log_quality('inpatient_drg', rows_in, rows_out, issues)
    return df


# ── DATASET 3: MEPS Conditions (Script 03) ────────────────

def clean_meps():
    section("DATASET 3 — MEPS Urology Conditions (meps_urology_conditions_2022.csv)")

    path = os.path.join(RAW, 'meps', 'meps_urology_conditions_2022.csv')
    df   = pd.read_csv(path, low_memory=False)
    rows_in = len(df)
    print(f"  Loaded: {rows_in:,} rows × {len(df.columns)} columns")
    issues = []

    # ── Rename key columns ──
    rename = {
        'DUPERSID'  : 'person_id',
        'CONDN'     : 'condition_number',
        'CONDIDX'   : 'condition_index',
        'ICD10CDX'  : 'icd10_code',
        'CCSR1X'    : 'ccsr_category_1',
        'CCSR2X'    : 'ccsr_category_2',
        'PANEL'     : 'panel',
        'AGEDIAG'   : 'age_at_diagnosis',
    }
    df.rename(columns={k: v for k, v in rename.items() if k in df.columns}, inplace=True)

    # ── Clean ICD-10 codes ──
    if 'icd10_code' in df.columns:
        df['icd10_code'] = df['icd10_code'].astype(str).str.strip().str.upper()
        # Remove invalid codes (CMS uses -15 = not ascertained)
        invalid_before = len(df)
        df = df[~df['icd10_code'].str.startswith('-')].copy()
        removed = invalid_before - len(df)
        if removed > 0:
            issues.append(f"removed {removed} invalid/missing ICD-10 codes")

    # ── Add ICD-10 chapter description ──
    ICD10_LABELS = {
        'N39': 'Urinary Tract Infection',
        'N40': 'Benign Prostatic Hyperplasia (BPH)',
        'N20': 'Kidney Stone (Urolithiasis)',
        'N28': 'Other Kidney Disorder',
        'R32': 'Urinary Incontinence',
        'C61': 'Prostate Cancer',
        'N32': 'Bladder Disorder',
        'N42': 'Other Prostate Disorder',
        'N52': 'Erectile Dysfunction',
        'N30': 'Cystitis (Bladder Infection)',
        'N19': 'Chronic Kidney Disease',
        'N18': 'Chronic Kidney Disease',
        'N17': 'Acute Kidney Failure',
    }
    if 'icd10_code' in df.columns:
        df['condition_label'] = df['icd10_code'].str[:3].map(ICD10_LABELS).fillna('Other Urology')

    df.drop_duplicates(inplace=True)

    rows_out = len(df)
    out_path = os.path.join(PROC, 'meps_clean.csv')
    df.to_csv(out_path, index=False)

    if 'condition_label' in df.columns:
        print(f"  Condition breakdown:")
        for label, cnt in df['condition_label'].value_counts().head(8).items():
            print(f"    {cnt:5,}  {label}")

    print(f"  Issues : {issues if issues else 'none'}")
    print(f"  Saved  : {out_path}")

    log_quality('meps_conditions', rows_in, rows_out, issues)
    return df


# ── DATASET 4: Hospital Compare (Script 04) ───────────────

def clean_hospital():
    section("DATASET 4 — Hospital Compare (hospital_compare_latest.csv)")

    path = os.path.join(RAW, 'cms_hospital', 'hospital_compare_latest.csv')
    df   = pd.read_csv(path, low_memory=False)
    rows_in = len(df)
    print(f"  Loaded: {rows_in:,} rows × {len(df.columns)} columns")
    issues = []

    # ── Rename ──
    rename = {
        'facility_id'             : 'hospital_id',
        'facility_name'           : 'hospital_name',
        'citytown'                : 'city',
        'hospital_overall_rating' : 'overall_rating',
        'hospital_type'           : 'hospital_type',
        'hospital_ownership'      : 'ownership',
        'emergency_services'      : 'has_emergency',
    }
    df.rename(columns={k: v for k, v in rename.items() if k in df.columns}, inplace=True)

    # ── Numeric ──
    if 'overall_rating' in df.columns:
        df['overall_rating'] = to_numeric_safe(df['overall_rating'])

    # ── Standardize text ──
    if 'state' in df.columns:
        df['state'] = df['state'].str.upper().str.strip()
    if 'hospital_name' in df.columns:
        df['hospital_name'] = df['hospital_name'].str.title().str.strip()

    # ── Flag academic medical centers ──
    AMC_KEYWORDS = ['university', 'medical center', 'johns hopkins',
                    'mayo', 'cleveland clinic', 'memorial sloan', 'academic']
    if 'hospital_name' in df.columns:
        name_lower = df['hospital_name'].str.lower()
        df['is_academic_medical_center'] = name_lower.apply(
            lambda n: any(kw in str(n) for kw in AMC_KEYWORDS)
        )

    df.drop_duplicates(subset=['hospital_id'] if 'hospital_id' in df.columns else None, inplace=True)

    rows_out = len(df)
    out_path = os.path.join(PROC, 'hospital_clean.csv')
    df.to_csv(out_path, index=False)

    if 'overall_rating' in df.columns:
        rated = df['overall_rating'].notna()
        print(f"  Hospitals with rating : {rated.sum():,}")
        print(f"  Average star rating   : {df['overall_rating'].mean():.2f}")
        print(f"  5-star hospitals      : {(df['overall_rating']==5).sum():,}")
        print(f"  1-star hospitals      : {(df['overall_rating']==1).sum():,}")

    print(f"  Issues : {issues if issues else 'none'}")
    print(f"  Saved  : {out_path}")

    log_quality('hospital_compare', rows_in, rows_out, issues)
    return df


# ── DATASET 5: Benchmarks (Script 05) ─────────────────────

def clean_benchmarks():
    section("DATASET 5 — National Benchmarks (cms_geo_urology_2022.csv)")

    path = os.path.join(RAW, 'benchmarks', 'cms_geo_urology_2022.csv')
    df   = pd.read_csv(path, low_memory=False)
    rows_in = len(df)
    print(f"  Loaded: {rows_in:,} rows × {len(df.columns)} columns")
    issues = []

    # ── Rename ──
    rename = {
        'Rndrng_Prvdr_Geo_Lvl'  : 'geo_level',
        'Rndrng_Prvdr_Geo_Cd'   : 'geo_code',
        'Rndrng_Prvdr_Geo_Desc' : 'geo_name',
        'HCPCS_Cd'              : 'cpt_code',
        'HCPCS_Desc'            : 'cpt_description',
        'Place_Of_Srvc'         : 'place_of_service',
        'Tot_Rndrng_Prvdrs'     : 'total_providers',
        'Tot_Benes'             : 'total_beneficiaries',
        'Tot_Srvcs'             : 'total_services',
        'Avg_Sbmtd_Chrg'        : 'avg_submitted_charge',
        'Avg_Mdcr_Alowd_Amt'    : 'avg_allowed_amount',
        'Avg_Mdcr_Pymt_Amt'     : 'avg_medicare_payment',
        'Avg_Mdcr_Stdzd_Amt'    : 'avg_standardized_payment',
    }
    df.rename(columns={k: v for k, v in rename.items() if k in df.columns}, inplace=True)

    # ── Numerics ──
    for col in ['total_providers', 'total_beneficiaries', 'total_services',
                'avg_submitted_charge', 'avg_allowed_amount',
                'avg_medicare_payment', 'avg_standardized_payment']:
        if col in df.columns:
            df[col] = to_numeric_safe(df[col])

    # ── Separate national vs state benchmarks ──
    if 'geo_level' in df.columns:
        national  = df[df['geo_level'] == 'National'].copy()
        state_lvl = df[df['geo_level'] == 'State'].copy()
        print(f"  National benchmark rows : {len(national):,}")
        print(f"  State benchmark rows    : {len(state_lvl):,}")

    df.drop_duplicates(inplace=True)

    rows_out = len(df)
    out_path = os.path.join(PROC, 'benchmarks_clean.csv')
    df.to_csv(out_path, index=False)

    # Show top CPT codes nationally
    if 'geo_level' in df.columns and 'cpt_code' in df.columns and 'total_services' in df.columns:
        nat = df[df['geo_level'] == 'National'].nlargest(8, 'total_services')
        print(f"\n  Top urology CPT codes nationally:")
        for _, row in nat.iterrows():
            desc = str(row.get('cpt_description', ''))[:40]
            print(f"    {row['cpt_code']:8s}  {row['total_services']:>12,.0f} svc  {desc}")

    print(f"\n  Issues : {issues if issues else 'none'}")
    print(f"  Saved  : {out_path}")

    log_quality('benchmarks_geo', rows_in, rows_out, issues)
    return df


# ── Main ──────────────────────────────────────────────────

def main():
    print("=" * 62)
    print("  SCRIPT 06 — Clean and Validate All Datasets")
    print("=" * 62)
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    df_physician  = clean_physician()
    df_inpatient  = clean_inpatient()
    df_meps       = clean_meps()
    df_hospital   = clean_hospital()
    df_benchmarks = clean_benchmarks()

    # ── Data Quality Report ───────────────────────────────
    section("DATA QUALITY REPORT")
    quality_df = pd.DataFrame(quality_log)
    print(quality_df.to_string(index=False))

    rpt_path = os.path.join(RPTS, 'data_quality_report.csv')
    quality_df.to_csv(rpt_path, index=False)
    print(f"\n  Saved quality report: {rpt_path}")

    # ── Final summary ─────────────────────────────────────
    total_rows = sum(q['rows_out'] for q in quality_log)
    print()
    print("=" * 62)
    print("  SCRIPT 06 COMPLETE — ALL DATASETS CLEANED")
    print(f"  Total clean rows across all files: {total_rows:,}")
    print()
    print("  data/processed/ now contains:")
    for f in ['physician_clean.csv', 'inpatient_clean.csv', 'meps_clean.csv',
              'hospital_clean.csv', 'benchmarks_clean.csv']:
        fpath = os.path.join(PROC, f)
        if os.path.exists(fpath):
            size = os.path.getsize(fpath) / 1024 / 1024
            print(f"    {f:30s}  {size:.2f} MB")
    print()
    print("  Next: python scripts/07_load_to_sqlite.py")
    print("=" * 62)

    append_audit_log('06_clean_and_validate.py', 'All 5 datasets',
                     sum(q['rows_in'] for q in quality_log),
                     total_rows, 'SUCCESS',
                     f"5 datasets cleaned, quality report saved")


if __name__ == "__main__":
    main()
