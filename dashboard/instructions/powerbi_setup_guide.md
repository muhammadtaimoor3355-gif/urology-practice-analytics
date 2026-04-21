# Power BI Dashboard Setup Guide
## Johns Hopkins Urology Analytics System

**Time to build**: ~45–60 minutes  
**Power BI Desktop**: Free download from microsoft.com/powerbi  
**Data folder**: `outputs/powerbi/` (17 CSV files)

---

## Step 1 — Load All Data Files

1. Open **Power BI Desktop**
2. Click **Home → Get Data → Text/CSV**
3. Load each file from `outputs/powerbi/` — do them one at a time:

| File | Table Name in Power BI |
|------|------------------------|
| `tab0_executive_overview.csv` | `KPI_Overview` |
| `tab1_physician_productivity.csv` | `Physician_Productivity` |
| `tab1_productivity_by_state.csv` | `Productivity_By_State` |
| `tab2_billing_analysis.csv` | `Billing_CPT` |
| `tab2_billing_anomalies.csv` | `Billing_Anomalies` |
| `tab2_cpt_benchmarks.csv` | `CPT_Benchmarks` |
| `tab3_condition_demand.csv` | `Condition_Demand` |
| `tab3_state_capacity.csv` | `State_Capacity` |
| `tab3_or_utilization.csv` | `OR_Utilization` |
| `tab3_los_benchmarks.csv` | `LOS_Benchmarks` |
| `tab4_benchmark_report.csv` | `Benchmark_Report` |
| `tab4_state_rankings.csv` | `State_Rankings` |
| `tab4_scorecard.csv` | `Performance_Scorecard` |
| `tab5_alerts.csv` | `Alerts` |
| `tab5_forecast.csv` | `Forecast` |
| `tab6_scenarios.csv` | `Scenarios` |
| `data_dictionary.csv` | `Data_Dictionary` |

For each file: click **Load** (not Transform — the data is already clean).

---

## Step 2 — Create Report Pages (Tabs)

Right-click the page tabs at the bottom → **Insert Page** for each:

1. `Overview`
2. `Physician Productivity`
3. `Billing Analysis`
4. `Capacity & Access`
5. `National Benchmarks`
6. `Alerts & Forecast`
7. `Scenario Modeling`

---

## Tab 0 — Overview (KPI Cards)

**Table**: `KPI_Overview`

**Visuals to add**:
- **Multi-row card** visual → Fields: `kpi`, `value`
  - Shows all 15 KPIs at once
- **Text box** at top: "Johns Hopkins Urology Analytics | FY 2022"

**Optional**: Add logo image (Insert → Image)

---

## Tab 1 — Physician Productivity

**Table**: `Physician_Productivity`

**Visuals**:

1. **Bar chart** — Top 20 physicians by revenue
   - Axis: `last_name` + `first_name`
   - Values: `estimated_annual_revenue`
   - Sort: descending

2. **Scatter plot** — Volume vs. Revenue
   - X-axis: `total_procedures`
   - Y-axis: `estimated_annual_revenue`
   - Legend: `performance_tier`
   - Tooltip: `last_name`, `state`

3. **Map** (filled or bubble) — Revenue by state
   - Table: `Productivity_By_State`
   - Location: `state`
   - Values: `avg_revenue_per_md`
   - Color saturation: darker = higher revenue

4. **Slicer** — `performance_tier`
   - Values: TOP_10, TOP_25, ABOVE_MEDIAN, BELOW_MEDIAN, BOTTOM_25

5. **KPI card** — "National Median Revenue/Physician: $89,500"

---

## Tab 2 — Billing Analysis

**Tables**: `Billing_CPT`, `Billing_Anomalies`

**Visuals**:

1. **Treemap** — CPT codes by total services
   - Group: `cpt_code`
   - Values: `total_services`
   - Tooltip: `cpt_description`, `total_revenue`

2. **Bar chart** — Top 15 CPTs by revenue
   - Axis: `cpt_code`
   - Values: `total_revenue`
   - Color: conditional formatting (red = anomaly)

3. **Table** — Billing anomalies
   - Table: `Billing_Anomalies`
   - Columns: `last_name`, `state`, `cpt_code`, `total_services`, `z_score`, `alert_level`
   - Conditional formatting on `alert_level`: RED = red, YELLOW = yellow

4. **Card** — "6,354 billing anomalies detected"

5. **Slicer** — `alert_level` (RED / YELLOW)

---

## Tab 3 — Capacity & Access

**Tables**: `State_Capacity`, `Condition_Demand`, `OR_Utilization`

**Visuals**:

1. **Filled map** — Capacity utilization by state
   - Location: `state`
   - Color saturation: `capacity_utilization_pct`
   - Color scale: green (0%) → yellow (80%) → red (150%+)

2. **Bar chart** — Top 10 urology conditions by demand
   - Table: `Condition_Demand`
   - Axis: `condition_label`
   - Values: `case_count`
   - Colors: blue = Ambulatory, orange = Inpatient/Mixed

3. **Gauge** — Capacity utilization (national average)
   - Value: average of `capacity_utilization_pct`
   - Min: 0, Max: 200, Target: 80

4. **Table** — OR utilization by DRG
   - Table: `OR_Utilization`
   - Columns: `drg_code`, `drg_description`, `total_or_cases`, `avg_payment`

5. **Card** — "21 states above 80% capacity"

---

## Tab 4 — National Benchmarks

**Tables**: `Performance_Scorecard`, `State_Rankings`

**Visuals**:

1. **Table** — Performance scorecard (main visual)
   - Table: `Performance_Scorecard`
   - Columns: `metric`, `value`, `benchmark`, `status`
   - Conditional formatting on `status`:
     - ABOVE → green background
     - BELOW → red background
     - WARNING → yellow background
     - INFO → white/grey

2. **Bar chart** — State rankings by revenue/MD
   - Table: `State_Rankings`
   - Axis: `state`
   - Values: `avg_revenue_per_md`
   - Sort: descending
   - Color: highlight MD bar in blue

3. **Card** — "Maryland: #6 of 51 states | +45.7% above national avg"

4. **Slicer** — `state` (to filter the ranking chart)

---

## Tab 5 — Alerts & Forecast

**Tables**: `Alerts`, `Forecast`

**Visuals**:

1. **Table** — Alert dashboard (main visual)
   - Table: `Alerts`
   - Columns: `alert_type`, `alert_level`, `last_name`, `state`, `message`
   - Conditional formatting on `alert_level`: RED = red, YELLOW = yellow
   - Sort: alert_level descending (RED first)

2. **Cards** (top row):
   - "2,372 RED Alerts"
   - "6,220 YELLOW Alerts"
   - "$161.8M Revenue at Risk"

3. **Line chart** — 6-month forecast
   - Table: `Forecast`
   - Axis: `forecast_month`
   - Values: `forecast_value`
   - Filter: `metric` = "Patient Volume (ambulatory procedures)"
   - Add error bars using `lower_80pct_ci` and `upper_80pct_ci`

4. **Slicer** — `alert_type`
   - Values: PRODUCTIVITY_WARNING, BILLING_ANOMALY, CAPACITY_CRISIS

5. **Slicer** — `metric` (for the forecast chart)

---

## Tab 6 — Scenario Modeling

**Table**: `Scenarios`

**Visuals**:

1. **Clustered bar chart** — Revenue by scenario (MAIN VISUAL)
   - Axis: `scenario`
   - Values: `total_annual_revenue`
   - Color: highlight BASELINE in grey, others in blue
   - Sort: manual order (BASELINE first)

2. **Table** — Full scenario comparison
   - Columns: `scenario`, `physicians`, `or_slots_per_week`, `medicare_rate_pct`,
     `total_annual_revenue`, `vs_baseline_revenue`, `total_annual_capacity`
   - Conditional formatting on `vs_baseline_revenue`:
     - Positive → green
     - Negative → red

3. **Cards** (insight row):
   - "Hire 2 MDs: +$895K/yr"
   - "Add 10 OR Slots: +$1.29M/yr"
   - "5% Rate Cut: -$308K/yr"

4. **Bar chart** — Capacity by scenario
   - Axis: `scenario`
   - Values: `total_annual_capacity`

---

## Step 3 — Theme & Formatting

**Recommended color scheme** (Johns Hopkins brand):
- Primary blue: `#002D72`
- Gold accent: `#C8AA6E`
- Alert RED: `#C00000`
- Alert YELLOW: `#FFC000`
- Background: `#F5F5F5`

**Apply theme**:
- View → Themes → Customize current theme
- Enter the hex codes above

**Add consistent header to each page**:
- Insert → Text box: "Johns Hopkins Urology Analytics | FY 2022"
- Font: Segoe UI, 14pt, color `#002D72`

---

## Step 4 — Publish (Optional)

1. **File → Save As** → `JHU_Urology_Analytics.pbix`
2. To share: **Home → Publish** → choose your Power BI workspace
3. Or export as PDF: **File → Export → PDF**

---

## Troubleshooting

| Problem | Fix |
|---------|-----|
| Map visual shows no data | Change `state` column data category to "State or Province" in Column tools |
| Numbers showing as text | In Power Query: change column type to Decimal/Whole Number |
| Forecast dates in wrong order | Sort `forecast_month` column as text ascending (YYYY-MM sorts correctly) |
| Slow performance | Disable Auto date/time in File → Options → Data Load |

---

## Interview Demo Script (2 minutes)

> "I built this system on five real government datasets — no synthetic data. Let me walk you through it.
>
> **Tab 1** shows Maryland ranks #6 nationally in urology revenue — our physicians earn 45.7% above the US average. That's the market Johns Hopkins operates in.
>
> **Tab 5** is the alert system — it automatically flagged 2,372 physicians and $161 million in revenue at risk. In a live system, department administrators would see this every Monday morning.
>
> **Tab 5 forecast** — this is ARIMA time-series modeling. It projects 19.8 million procedures and $550 million in revenue over the next six months, with 80% confidence intervals.
>
> **Tab 6** answers the question every administrator asks: 'Should we hire more doctors or expand OR time?' The model shows adding 10 OR slots per week generates $1.29 million more per year — higher ROI than hiring, and faster to implement.
>
> The whole system is reproducible: one command — `python run_all.py` — rebuilds everything from scratch."

---

*Data: CMS Open Data 2022 | MEPS H241 2022 | CMS Hospital Compare 2022*  
*All data is publicly available. No patient-level data used.*
