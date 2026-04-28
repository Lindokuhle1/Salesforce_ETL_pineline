# Sales Performance App Presentation Notes

## 1. Opening Summary (30-45 seconds)

This solution is an end-to-end sales performance platform that starts from Excel source data and delivers three business outputs:

- a normalized PostgreSQL analytics model,
- a live React dashboard for operational monitoring,
- and auto-generated manager reports in Excel and HTML for email sharing.

The core value is turning raw monthly KPI data into decision-ready insights for sales managers, CAO, and board stakeholders.

---

## 2. What Problem This Solves

Before this solution, reporting from raw spreadsheets is manual, slow, and inconsistent.

This app solves that by:

- standardizing data quality checks before load,
- centralizing KPIs in a relational model,
- providing near-live visual monitoring,
- generating presentation-ready and email-ready reports automatically.

---

## 3. Architecture in Plain Language

Pipeline flow:

1. Data Quality Stage
- validates required columns,
- checks key data quality rules,
- writes dq_report.json.

2. ETL Stage
- extracts from Excel,
- cleans and maps fields,
- loads PostgreSQL dimensions and fact table,
- refreshes reporting view.

3. Export Stage
- generates Power BI-ready files into powerbi_exports.

4. Dashboard Sync Stage
- copies latest exports to the React app data folder for frontend refresh.

5. Manager Report Stage
- creates reports/manager_performance_report.xlsx,
- creates reports/manager_performance_report.html for email distribution.

---

## 4. Frontend Dashboard Highlights

The React dashboard includes:

- KPI cards (attainment, variance, YTD, pipeline, status counts),
- manager and rep drilldown filters,
- region, monthly, manager, and rep trends,
- executive visuals for risk and concentration,
- responsive layout for desktop/tablet/mobile,
- auto-refresh behavior using export metadata polling.

Business behavior:

- If a manager is selected, visuals scope to that manager.
- If a rep is selected, visuals scope to that rep.

---

## 5. Manager Report Pack (Email-Ready)

Generated outputs:

- Excel workbook with multiple sheets and native charts,
- HTML report with KPI blocks, visual charts, and summary tables.

Report content:

- manager attainment and variance summary,
- monthly trend,
- top reps,
- at-risk/off-track snapshot.

Why this matters:

- executives get a quick visual summary,
- managers receive actionable performance detail,
- delivery format supports both attachment-based and inline-email workflows.

---

## 6. Demo Flow (5-7 minutes)

1. Show orchestration command
- Run: python run_orchestration.py
- Explain that one command executes quality checks, ETL, exports, dashboard sync, and manager report generation.

2. Show run artifacts
- dq_report.json
- orchestration_runs/latest_run.json
- reports/manager_performance_report.xlsx
- reports/manager_performance_report.html

3. Open React dashboard
- Show KPI cards,
- filter by manager,
- filter by rep,
- explain scoped visuals and risk indicators.

4. Open generated reports
- Excel: show manager summary and chart tabs.
- HTML: show email-friendly visuals and table summaries.

5. Close with impact
- faster decision cycles,
- consistent data governance,
- reusable reporting pipeline.

---

## 7. Talking Points for Q&A

Technical strength:

- modular Python design,
- orchestration with stage-level status and failure handling,
- documented, repeatable artifacts.

Data reliability:

- pre-load quality validation,
- controlled schema mapping,
- deterministic full refresh behavior.

Business value:

- one workflow supports operations and executive reporting,
- reduced manual report prep time,
- improved visibility into manager and rep performance.

---

## 8. Suggested Closing Statement

This project is not only an ETL script or a dashboard. It is a full reporting product: validated data pipeline, operational analytics UI, and automated leadership reporting outputs that can be shared immediately.
