# Infrastructure Document
## Balancell Sales Performance ETL and BI Platform

## 1. Purpose
This document describes the technical infrastructure for the Balancell sales performance solution built with Python ETL, PostgreSQL, and Power BI. It is intended for interview discussion, handover, and operational reference.

The platform now includes a full orchestration layer with automated data quality validation, ETL load, and Power BI dataset export — all coordinated by a single entry-point runner with a structured run summary output.

## 2. Scope
In scope:
- Data ingestion from Excel source file
- Automated data quality checks before load
- ETL processing in Python
- Relational storage in PostgreSQL
- Power BI CSV/Parquet dataset export
- Full pipeline orchestration with run summaries
- Security, operations, and reliability controls

Out of scope:
- CRM/source system integration beyond Excel
- Enterprise IAM/SSO implementation
- Multi-region failover architecture

## 3. High-Level Architecture
```text
+-------------------------------------+
| Orchestrator                        |
| run_orchestration.py                |
| - Coordinates all stages            |
| - Fail-fast on errors               |
| - Writes orchestration_runs/        |
|   latest_run.json                   |
+------+----------+-------------------+
       |          |                   |
       | Stage 1  | Stage 2           | Stage 3
       v          v                   v
+------------+ +------------------+ +--------------------+
| Data       | | Python ETL       | | Power BI Export    |
| Quality    | | Runtime          | | powerbi_export.py  |
|            | |                  | |                    |
| data_      | | sales_report.py  | | Queries            |
| quality_   | | pandas +         | | vw_powerbi_sales_  |
| checks.py  | | SQLAlchemy       | | performance        |
|            | |                  | |                    |
| Reads      | | Reads            | | Writes             |
| source     | | source Excel     | | powerbi_exports/   |
| Excel      | |                  | | *.csv or *.parquet |
|            | |                  | |                    |
| Writes     | | Loads to         | | Writes             |
| dq_report  | | PostgreSQL       | | export_metadata    |
| .json      | |                  | | .txt               |
+-----+------+ +-------+----------+ +--------------------+
      |                 |
      | Validate        | Load
      v                 v
+---------------------+-----------------------------+
| Source Data         | PostgreSQL                  |
| sales_performance   | DB: balancell_sales_analysis|
| _v2.xlsx            | Tables:                     |
|                     | - regions                   |
| 480 rows            | - sales_managers            |
| 21 columns          | - sales_reps                |
| 40 reps             | - sales_perfomance          |
| 5 regions           | View:                       |
| 12 months           | - vw_powerbi_sales_         |
+---------------------+   performance               |
                      +----+------------------------+
                           |
                           | Query (direct) or
                           | via exported CSV/Parquet
                           v
                      +----------------------------+
                      | Power BI                   |
                      | Import mode (recommended)  |
                      | Dashboards and KPIs        |
                      +----------------------------+
```

## 4. Logical Data Model
- regions: unique region dimension
- sales_managers: manager linked to region via region_id
- sales_reps: rep linked to manager via manager_id
- sales_perfomance: monthly KPI fact linked to rep_id
- vw_powerbi_sales_performance: flattened reporting view for BI

Referential chain:
- sales_perfomance.rep_id -> sales_reps.id
- sales_reps.manager_id -> sales_managers.id
- sales_managers.region_id -> regions.id

## 5. Runtime Components

| Script | Role |
|---|---|
| `run_orchestration.py` | Orchestrator — entry point for all pipeline stages |
| `data_quality_checks.py` | Stage 1 — validates source data before load |
| `sales_report.py` | Stage 2 — ETL: Extract, Transform, Load to PostgreSQL |
| `powerbi_export.py` | Stage 3 — exports Power BI-ready datasets from PostgreSQL |

- Application runtime: Python 3.10+
- ETL libraries: pandas, SQLAlchemy, psycopg2-binary, openpyxl
- Database: PostgreSQL 13+
- Reporting: Power BI Desktop or Power BI Service
- OS: Windows (current environment)

## 6. Environment Configuration
Environment variables used by ETL:
- INPUT_FILE (default: sales_performance_v2.xlsx)
- PG_HOST (default: 127.0.0.1)
- PG_PORT (default: 5432)
- PG_DATABASE (default: balancell_sales_analysis)
- PG_USER (default: postgres)
- PG_PASSWORD (required in production)
- PG_BOOTSTRAP_DB (default: postgres)

Configuration principles:
- No hardcoded secrets in source control
- Use per-environment values (dev/test/prod)
- Restrict DB credentials to minimum required privileges

## 7. Deployment Topology
### 7.1 Current (Local Development)
- ETL runs manually from terminal on analyst/developer machine
- PostgreSQL runs locally
- Power BI connects directly to local PostgreSQL

### 7.2 Recommended Production Topology
- ETL host: dedicated Windows server or Linux VM/container
- PostgreSQL host: managed PostgreSQL service or hardened DB VM
- Power BI gateway: installed if Power BI Service needs private DB access
- Scheduler: Windows Task Scheduler, cron, or orchestration tool (Airflow)

## 8. Data Flow and Processing

### Stage 1 — Data Quality (`data_quality_checks.py`)
1. Read source Excel file
2. Normalize and clean column names
3. Run checks: non-null keys, valid months, non-negative metrics, valid status values, unique rep+month combinations
4. Write `dq_report.json` with pass/fail results per check
5. Fail-fast: orchestrator stops here if any check fails (unless `--continue-on-dq-fail` is set)

### Stage 2 — ETL Load (`sales_report.py`)
1. Read and clean source Excel file
2. Normalize column names and map aliases to canonical names
3. Build relational model: regions, managers, reps, fact rows
4. Ensure database and tables exist (auto-created if missing)
5. Truncate all tables in FK-safe order (RESTART IDENTITY CASCADE)
6. Load dimensions first, then fact table
7. Create/refresh `vw_powerbi_sales_performance` view
8. Log row counts per table

### Stage 3 — Power BI Export (`powerbi_export.py`)
1. Connect to PostgreSQL
2. Run four pre-built queries: `sales_detail`, `kpi_by_region`, `kpi_by_manager`, `kpi_by_month`
3. Write output files to `powerbi_exports/` as CSV or Parquet
4. Write `export_metadata.txt` with timestamp, database, format, and file list

### Orchestration Summary
- `run_orchestration.py` coordinates all three stages in order
- Writes `orchestration_runs/latest_run.json` with per-stage status, duration, and messages
- Overall status is `success` only if all enabled stages succeed

Load strategy:
- Full refresh (idempotent): safe and simple for current dataset size

## 9. Security Controls
- Secrets handling:
  - Store PG_PASSWORD in environment variables or secret vault
- Network controls:
  - Restrict PostgreSQL port access to trusted hosts only
  - Disable remote superuser login where possible
- Database controls:
  - Create dedicated ETL user with least privilege
  - Keep DDL rights separate from DML rights if governance requires
- Data protection:
  - Enable TLS for DB connections in non-local environments
  - Encrypt disk/volume where DB and source files are stored

## 10. Reliability and Recovery
### 10.1 Backup Strategy
- Daily logical backup of balancell_sales_analysis using pg_dump
- Weekly full backup retention snapshot
- Keep backup retention aligned with business policy (for example 30-90 days)

### 10.2 Restore Procedure (High Level)
1. Provision clean PostgreSQL instance/database
2. Restore latest valid backup
3. Re-run ETL for latest source file
4. Validate row counts and key KPI totals

### 10.3 Failure Handling
- If ETL fails before load: previous DB data remains intact
- If ETL fails during load: transaction boundaries and rerun process recover state
- Operator action: correct root cause, rerun ETL, verify row counts

## 11. Monitoring and Observability

Built-in observability (current implementation):
- `dq_report.json` — per-check pass/fail results with details after every data quality stage
- `orchestration_runs/latest_run.json` — structured run summary with stage name, status, start/end timestamps, duration in seconds, and error message
- Console log output for every stage with timestamps and log level

Run summary schema (`latest_run.json`):
```json
{
  "run_id": "20260423T120000Z",
  "generated_at_utc": "...",
  "overall_status": "success | failed",
  "stages": [
    {
      "name": "data_quality | etl | powerbi_export",
      "status": "success | failed",
      "started_at_utc": "...",
      "ended_at_utc": "...",
      "duration_seconds": 1.234,
      "message": "..."
    }
  ]
}
```

Recommended enhancements:
- Persist run summary to a PostgreSQL audit table for trend tracking
- Alert on failures via email or Teams webhook
- Archive run summaries by date (e.g. `orchestration_runs/20260423T120000Z.json`)

## 12. Performance and Scaling
Current dataset is small (hundreds of rows), so full refresh is efficient.

Scaling path:
- Add indexes on foreign keys and frequently filtered columns
- Move to incremental upsert by rep_id + month key
- Partition fact table by month/year when volume grows
- Separate ETL runtime from analyst workstation

## 13. Operational Runbook

### 13.1 Standard Run (Full Orchestration)
1. Ensure source file is present: `sales_performance_v2.xlsx`
2. Set environment variables:
   ```powershell
   $env:PG_PASSWORD = "your_password_here"
   ```
3. Run the orchestrator:
   ```powershell
   python run_orchestration.py
   ```
4. Check `orchestration_runs/latest_run.json` for per-stage status
5. Check `dq_report.json` for data quality results
6. Confirm `powerbi_exports/` contains four CSV files
7. Refresh Power BI report (Get Data → CSV or via PostgreSQL direct)

### 13.2 Run Individual Stages
| Goal | Command |
|---|---|
| Data quality only | `python data_quality_checks.py` |
| ETL load only | `python sales_report.py` |
| Power BI export only | `python powerbi_export.py` |
| Skip data quality | `python run_orchestration.py --skip-dq` |
| Skip export | `python run_orchestration.py --skip-export` |
| Ignore DQ failures | `python run_orchestration.py --continue-on-dq-fail` |

### 13.3 Post-Run Validation Checklist
- `orchestration_runs/latest_run.json` overall_status = `success`
- `dq_report.json` passed = true
- `powerbi_exports/sales_detail.csv` row count matches 480
- `powerbi_exports/kpi_by_region.csv` has 5 rows
- PostgreSQL: regions, sales_managers, sales_reps, sales_perfomance all have rows
- `vw_powerbi_sales_performance` query returns rows

### 13.4 Common Issues
| Symptom | Cause | Fix |
|---|---|---|
| Authentication failed | Wrong PG_PASSWORD | Set correct `$env:PG_PASSWORD` |
| Connection refused | PostgreSQL not running | Start PostgreSQL; verify PG_HOST and PG_PORT |
| Missing file | Excel file not found | Confirm `sales_performance_v2.xlsx` is in the script folder |
| DQ checks failed | Bad source data | Review `dq_report.json` for failing check details |
| Export DB error | ETL not run first | Run orchestrator or `sales_report.py` before `powerbi_export.py` |
| Empty Power BI report | Not refreshed | Reload data source in Power BI Desktop |

## 14. Non-Functional Requirements Snapshot
This section has been renumbered to 15. The Power BI setup guide is now Section 14 below.

---

## 14. Power BI Step-by-Step Setup Guide

### Step 1 — Install the PostgreSQL connector for Power BI
1. Open **Power BI Desktop**
2. Click **File → Options and settings → Options → Security**
3. Under **Data Extensions**, select **Allow any extension to load without validation or warning**
4. Download and install the **Npgsql** PostgreSQL driver if not already present:
   - https://github.com/npgsql/npgsql/releases
   - Install the latest **.msi** targeting **.NET Framework**
5. Restart Power BI Desktop after installation

---

### Step 2 — Connect to your PostgreSQL database
1. Open **Power BI Desktop**
2. Click **Home → Get Data → More...**
3. Search for **PostgreSQL** and select it, then click **Connect**
4. Fill in the connection dialog:
   - **Server:** `127.0.0.1`
   - **Database:** `balancell_sales_analysis`
5. Click **OK**
6. When prompted for credentials:
   - Select the **Database** tab on the left
   - **User name:** `postgres`
   - **Password:** *(your PostgreSQL password)*
7. Click **Connect**

---

### Step 3 — Select tables or view to import
In the **Navigator** window you will see all objects in `balancell_sales_analysis`.

**Option A — Use the flat view (easiest, recommended for this report)**
- Tick `vw_powerbi_sales_performance`
- Click **Load**

**Option B — Use all four tables (for full star model control)**
- Tick all four:
  - `regions`
  - `sales_managers`
  - `sales_reps`
  - `sales_perfomance`
- Click **Load**

---

### Step 4 — Set relationships (Option B only)
If you loaded the four individual tables:

1. Click the **Model** icon on the left sidebar (looks like three connected boxes)
2. Power BI may auto-detect relationships. If not, create them manually:
   - Drag `sales_perfomance.rep_id` → `sales_reps.id`
   - Drag `sales_reps.manager_id` → `sales_managers.id`
   - Drag `sales_managers.region_id` → `regions.id`
3. Set each relationship cardinality to **Many to One (\*)→1**
4. Set cross-filter direction to **Single**

---

### Step 5 — Create DAX measures
Click on the `sales_perfomance` table in the **Fields** pane, then **New Measure** for each:

```dax
Revenue Attainment % =
  DIVIDE(SUM(sales_perfomance[rev_actual]), SUM(sales_perfomance[rev_target]), 0)

Revenue vs Target =
  SUM(sales_perfomance[rev_actual]) - SUM(sales_perfomance[rev_target])

YTD Revenue =
  SUM(sales_perfomance[ytd_actual])

Pipeline Coverage =
  AVERAGE(sales_perfomance[pipeline_coverage])

Total Pipeline Value =
  SUM(sales_perfomance[pipeline_value])

On Track Count =
  CALCULATE(COUNTROWS(sales_perfomance), SEARCH("On Track", sales_perfomance[status], 1, 0) > 0)

Off Track Count =
  CALCULATE(COUNTROWS(sales_perfomance), SEARCH("Off Track", sales_perfomance[status], 1, 0) > 0)

At Risk Count =
  CALCULATE(COUNTROWS(sales_perfomance), SEARCH("At Risk", sales_perfomance[status], 1, 0) > 0)
```

---

### Step 6 — Build the report page by page

#### Page 1 — Executive Overview
| Visual | Fields to use |
|---|---|
| KPI Card | `Revenue Attainment %` |
| KPI Card | `YTD Revenue` |
| KPI Card | `Total Pipeline Value` |
| KPI Card | `Pipeline Coverage` |
| Donut chart | Legend: `status`, Values: count of `performance_id` |
| Bar chart | Axis: `region`, Values: `rev_actual` and `rev_target` |

How to add a KPI Card:
1. Click the **Card** visual in the Visualizations pane
2. Drag your measure into **Fields**
3. Format → Data label → set decimal places and currency prefix if needed

#### Page 2 — Sales Rep Performance
| Visual | Fields to use |
|---|---|
| Matrix | Rows: `sales_manager` then `sales_rep`, Values: `rev_actual`, `rev_target`, `Revenue Attainment %` |
| Conditional formatting | On `Revenue Attainment %` → Color scale: Red (0%) → Green (100%+) |

| Slicer | Field: `region` |
| Slicer | Field: `month` |

How to add conditional formatting to a matrix:
1. Click the matrix visual
2. In the **Format** pane → **Cell elements**
3. Turn on **Background color** for the attainment column
4. Click **fx** → choose **Color scale** → set Min to red, Max to green

#### Page 3 — Monthly Trend
| Visual | Fields to use |
|---|---|
| Line chart | X Axis: `month`, Values: `rev_actual`, Legend: `sales_rep` |
| Line chart | X Axis: `month`, Values: `pipeline_value` |
| Clustered bar chart | Axis: `month`, Values: `visit_made` and `visit_target` |
| Clustered bar chart | Axis: `month`, Values: `quotes_made` and `quote_target` |

#### Page 4 — Pipeline Analysis
| Visual | Fields to use |
|---|---|
| Scatter plot | X: `rev_actual`, Y: `pipeline_value`, Size: `pipeline_coverage`, Details: `sales_rep` |
| Table | `sales_rep`, `pipeline_value`, `pipeline_coverage`, `status` |
| Slicer | `status` — filter to At Risk / Off Track to surface urgent reps |

---

### Step 7 — Apply report-level formatting
1. **View → Themes** — choose a built-in theme or import a custom one
2. **Format → Page background** — set to light grey or white
3. Click each visual → **Format → Border** → enable subtle border for polish
4. Add a **Text Box** at the top of each page as the page title
5. Add a logo: **Insert → Image** and select a company logo file

---

### Step 8 — Set up data refresh
**Option A — Manual refresh (current setup)**
1. Click **Home → Refresh**
2. Power BI re-queries PostgreSQL and updates all visuals

**Option B — Scheduled refresh via Power BI Service**
1. Publish the report: **Home → Publish → My Workspace**
2. In Power BI Service (app.powerbi.com):
   - Go to the dataset → **Settings**
   - Install **On-premises data gateway** (required for local PostgreSQL)
   - Under **Scheduled refresh**, enable and set a daily refresh time

---

### Step 9 — Save and share
1. **File → Save As** → save as `Balancell_Sales_Performance.pbix`
2. Static sharing: **File → Export → Export to PDF**
3. Live sharing: Publish to Power BI Service and share the workspace link with stakeholders

---

- Availability target: business hours reporting availability
- RPO target: <= 24 hours (daily backup)
- RTO target: <= 4 hours for restore and validation
- Data latency target: daily batch or on-demand batch

## 15. Future Improvements
- Introduce CI/CD for ETL deployments
- Add containerized runtime (Docker)
- Implement secret manager integration
- Add orchestration and retries with Airflow
- Add tests for schema mapping and data quality assertions
