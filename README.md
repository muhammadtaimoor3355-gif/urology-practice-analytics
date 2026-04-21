# Johns Hopkins Urology Analytics System
### Practice Performance Analytics with Predictive Intelligence

A complete healthcare business analytics portfolio project built entirely on **real government data** (CMS, MEPS, AHRQ). Designed for the Business Analyst (Urology) position at Johns Hopkins Hospital.

---

## What This Project Does

Analyzes 8,940 urologists across 51 states using 162,360 CMS billing records to produce 7 analytics modules:

| Module | Script | What It Answers |
|--------|--------|-----------------|
| Physician Productivity | `08` | Which doctors are top performers vs. at-risk? |
| Billing Analysis | `09` | Which CPT codes drive revenue? Any anomalies? |
| Capacity & Access | `10` | Can we handle current patient demand? |
| National Benchmarking | `11` | How does Maryland compare to the US? |
| Predictive Alerts | `12` | What needs attention right now? |
| ARIMA Forecasting | `13` | What does the next 6 months look like? |
| Scenario Modeling | `14` | What happens if we hire 2 MDs / add OR slots? |

---

## Key Findings

- **Maryland ranks #6 nationally** in urology revenue — 45.7% above the national average
- **National median physician revenue**: $89,500/yr (Medicare-reimbursed)
- **2,372 RED alerts** generated; $161.8M in revenue at risk from productivity gaps
- **6-month forecast**: 19.8M procedures | $550M revenue (ARIMA with seasonal adjustment)
- **Hiring 2 MDs** adds +$895K/yr revenue and +10,000 patient slots
- **Adding 10 OR slots/week** is the highest-ROI lever: +$1.29M/yr at no hiring cost

---

## Data Sources (100% Real, 100% Free)

| Dataset | Source | Rows |
|---------|--------|------|
| CMS Physician & Other Supplier Billing 2022 | data.cms.gov | 162,360 |
| CMS Inpatient Hospitals DRG Data 2022 | data.cms.gov | 13,983 |
| MEPS Medical Conditions H241 2022 | meps.ahrq.gov | 2,696 |
| CMS Hospital Compare | data.cms.gov | 5,426 |
| CMS Geography & Service Benchmarks | data.cms.gov | 11,726 |

---

## How to Run

### Full pipeline (first time)
```bash
python run_all.py
```

### Resume from a specific script
```bash
python run_all.py --start 08
```

### Run a single script
```bash
python scripts/08_analysis_productivity.py
```

**Runtime**: ~10–15 minutes total (downloads + processing)  
**Requirements**: Python 3.8+, see `requirements.txt`

---

## Project Structure

```
urology-analytics/
│
├── run_all.py                    ← Master pipeline runner
│
├── scripts/
│   ├── 00_setup_environment.py   ← Create folders, audit trail
│   ├── 01–05_download_*.py       ← Download all datasets from APIs
│   ├── 06_clean_and_validate.py  ← Clean, rename, validate
│   ├── 07_load_to_sqlite.py      ← Load to SQLite database
│   ├── 08–14_analysis_*.py       ← 7 analytics modules
│   ├── 15_generate_powerbi_exports.py
│   ├── 16_generate_executive_memo.py
│   └── 17_run_audit_report.py
│
├── data/
│   ├── raw/                      ← Downloaded source files
│   └── processed/
│       └── master_database.sqlite  ← 63.5 MB, 5 tables, 195K+ rows
│
└── outputs/
    ├── reports/                  ← 15 analysis CSVs + executive memo
    └── powerbi/                  ← 17 Power BI-ready CSV files
```

---

## Power BI Dashboard

Load files from `outputs/powerbi/` — one tab per module:

| Tab | File | Visuals |
|-----|------|---------|
| Overview | `tab0_executive_overview.csv` | KPI cards |
| Physician Productivity | `tab1_physician_productivity.csv` | Bar chart, scatter, map |
| Billing Analysis | `tab2_billing_analysis.csv` | Treemap, anomaly table |
| Capacity & Access | `tab3_state_capacity.csv` | Choropleth map, gauge |
| National Benchmarks | `tab4_scorecard.csv` | Scorecard table, rank chart |
| Alerts & Forecast | `tab5_alerts.csv` + `tab5_forecast.csv` | Alert table, line forecast |
| Scenario Modeling | `tab6_scenarios.csv` | What-if bar chart |

See `dashboard/instructions/powerbi_setup_guide.md` for step-by-step setup.

---

## Technical Stack

- **Python 3.8+** — pandas, numpy, statsmodels (ARIMA), requests, sqlite3
- **SQLite** — lightweight data warehouse (no server needed)
- **Power BI Desktop** — dashboard visualization (free)
- **CMS Open Data API** — paginated REST API with URL-encoded filters
- **ARIMA(1,1,1)** — statsmodels time series forecasting

---

## Interview Talking Points

1. **Why real data?** Because synthetic data can't reveal real patterns — UTI being #1 urology demand driver, Maryland's 45.7% revenue premium, and the 10-OR-slot ROI insight all came from real data.

2. **ARIMA with single-year data?** Used state cross-sectional variation + published HCUP seasonal indexes as a monthly proxy. Standard approach in health services research. Transparent about the limitation.

3. **Alert system design?** Three-tier (RED/YELLOW/INFO) modeled on clinical severity frameworks — familiar to urology department administrators.

4. **What would you add with more data?** Rolling monthly claims for true longitudinal forecasting; patient wait-time data for access gap validation; payer mix breakdown for margin analysis.

---

*Built with CMS Open Data, MEPS H241, and AHRQ public datasets — no private patient data, no paid subscriptions.*
