#   Sales Performance — ETL Pipeline

A Python ETL pipeline that extracts sales KPI data from Excel, transforms it into a normalized relational model, loads it into PostgreSQL, and exposes a flat view ready for Power BI reporting.

---

## Project Structure

```
SALESFORCE/
├── sales_report.py           # ETL pipeline
├── powerbi_export.py         # Power BI dataset export script
├── data_quality_checks.py    # Optional data quality checks
├── sales_performance_v2.xlsx # Source data (480 rows, 21 columns)
└── README.md
```

---

## Prerequisites

| Requirement | Version |
|---|---|
| Python | 3.10+ |
| PostgreSQL | 13+ |
| Power BI Desktop | Any current version |

### Install Python dependencies

```powershell
pip install pandas sqlalchemy psycopg2-binary openpyxl
```

---

## Database Setup

Run the following SQL in pgAdmin or psql to create the target database and schema before running the pipeline.

```sql
CREATE DATABASE  _sales_analysis;

\c  _sales_analysis

CREATE TABLE IF NOT EXISTS regions (
    id   SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS sales_managers (
    id        SERIAL PRIMARY KEY,
    name      TEXT NOT NULL,
    region_id INTEGER REFERENCES regions(id)
);

CREATE TABLE IF NOT EXISTS sales_reps (
    id         SERIAL PRIMARY KEY,
    name       TEXT UNIQUE NOT NULL,
    manager_id INTEGER REFERENCES sales_managers(id)
);

CREATE TABLE IF NOT EXISTS sales_perfomance (
    id                SERIAL PRIMARY KEY,
    rep_id            INTEGER REFERENCES sales_reps(id),
    month             TEXT,
    rev_target        NUMERIC,
    rev_actual        NUMERIC,
    rev_attainment    NUMERIC,
    rev_variance      NUMERIC,
    visit_target      INTEGER,
    visit_made        INTEGER,
    visit_attainment  NUMERIC,
    call_target       INTEGER,
    calls_made        INTEGER,
    call_attainment   INTEGER,
    quote_target      INTEGER,
    quotes_made       INTEGER,
    quote_attainment  NUMERIC,
    pipeline_value    NUMERIC,
    pipeline_coverage NUMERIC,
    ytd_actual        NUMERIC,
    status            TEXT
);
```

> **Note:** The pipeline also creates these tables automatically if they don't exist, so this step is optional.

---

## Configuration

The pipeline reads all connection settings from **environment variables**. If a variable is not set, the default value below is used.

| Variable | Default | Description |
|---|---|---|
| `INPUT_FILE` | `sales_performance_v2.xlsx` | Source Excel filename (relative to script) |
| `PG_HOST` | `127.0.0.1` | PostgreSQL host |
| `PG_PORT` | `5432` | PostgreSQL port |
| `PG_DATABASE` | ` _sales_analysis` | Target database |
| `PG_USER` | `postgres` | Database user |
| `PG_PASSWORD` | *(your password)* | Database password |
| `PG_BOOTSTRAP_DB` | `postgres` | Admin database used to create the target DB if missing |

### Set variables in PowerShell

```powershell
$env:PG_HOST     = "127.0.0.1"
$env:PG_PORT     = "5432"
$env:PG_DATABASE = " _sales_analysis"
$env:PG_USER     = "postgres"
$env:PG_PASSWORD = "your_password_here"
```

---

## Running the Pipeline

```powershell
python sales_report.py
```

### One-command full orchestration (recommended)

Run quality checks, ETL load, and Power BI export in one command:

```powershell
python run_orchestration.py
```

Useful options:

- `python run_orchestration.py --skip-dq`
- `python run_orchestration.py --skip-export`
- `python run_orchestration.py --continue-on-dq-fail`
- `python run_orchestration.py --output-dir powerbi_exports --format csv`

Generated orchestration artifacts:

- `dq_report.json`
- `powerbi_exports/*`
- `reports/manager_performance_report.xlsx`
- `reports/manager_performance_report.html`
- `orchestration_runs/latest_run.json`

### Expected output

```
2026-04-20 15:58:32 | INFO | Starting ETL pipeline
2026-04-20 15:58:32 | INFO | Source file: ...\sales_performance_v2.xlsx
2026-04-20 15:58:40 | INFO | Loading regions (5 rows)...
2026-04-20 15:58:40 | INFO | Loading sales_managers (5 rows)...
2026-04-20 15:58:40 | INFO | Loading sales_reps (40 rows)...
2026-04-20 15:58:40 | INFO | Loading sales_perfomance (480 rows)...
2026-04-20 15:58:41 | INFO | ETL pipeline completed successfully
2026-04-20 15:58:41 | INFO | Power BI objects ready: regions, sales_managers, sales_reps, sales_perfomance, vw_powerbi_sales_performance
```

### Export Power BI-ready files with Python

After loading data to PostgreSQL, export ready-to-import datasets for Power BI:

```powershell
python powerbi_export.py --output-dir powerbi_exports --format csv
```

This generates:

- `powerbi_exports/sales_detail.csv`
- `powerbi_exports/kpi_by_region.csv`
- `powerbi_exports/kpi_by_manager.csv`
- `powerbi_exports/kpi_by_month.csv`
- `powerbi_exports/export_metadata.txt`

Use these files in Power BI via **Get Data -> Text/CSV** for quick demos without direct DB connectivity.

---

## React Frontend Dashboard

A React dashboard is available in the `sales-dashboard` folder. It reads the CSV exports generated by the ETL pipeline and renders KPI cards, trend charts, status distribution, and at-risk rep tables.

Security and compliance demo features in the frontend:

- Login screen (session-based UI gate)
- POPIA and ISO 27001-aligned control cards in the dashboard
- Live notification center for governance and operational events

### Run the frontend

1. Generate fresh exports and auto-sync dashboard data:

```powershell
python run_orchestration.py
```

By default orchestration now copies exported datasets into `sales-dashboard/public/data`.

2. Start the frontend:

```powershell
cd sales-dashboard
npm install
npm run dev
```

3. Open the local URL shown by Vite (usually http://localhost:5173).

4. The dashboard automatically checks for new data every 30 seconds and refreshes when `export_metadata.txt` changes.

### SignalR live notifications

To connect to a real SignalR hub, set the frontend environment variable:

```powershell
$env:VITE_SIGNALR_HUB_URL = "https://your-host/hubs/notifications"
```

Then start Vite in the same terminal session:

```powershell
npm run dev
```

Supported incoming hub events:

- `SalesRepAdded`
- `SalesManagerAdded`
- `Notification`

If no hub URL is provided, the app runs in demo notification mode.

Optional orchestration flags:

- `--skip-dashboard-sync` to disable dashboard file sync
- `--dashboard-data-dir <path>` to change the frontend data folder
- `--skip-manager-report` to disable report generation
- `--reports-dir <path>` to change report output folder

---

## Manager Email Report (Excel + HTML)

The pipeline can generate manager performance reports suitable for distribution:

- Excel workbook: `reports/manager_performance_report.xlsx`
- HTML report: `reports/manager_performance_report.html`

Both files are produced automatically by `python run_orchestration.py` unless `--skip-manager-report` is provided.

You can also run the report step directly:

```powershell
python manager_report.py --output-dir powerbi_exports --reports-dir reports --format csv
```

The Excel file includes summary sheets and chart tabs for manager attainment and monthly trend. The HTML file includes KPI cards, visual charts, manager summary table, top reps, and at-risk/off-track snapshot for easy mail sharing.

---

## Data Model

### Source → Target column mapping

| Excel Column | Normalized Column | Target Table |
|---|---|---|
| Region | name | regions |
| Sales Manager | name | sales_managers |
| Sales Rep | name | sales_reps |
| Month | month | sales_perfomance |
| Rev Target ($) | rev_target | sales_perfomance |
| Rev Actual ($) | rev_actual | sales_perfomance |
| Rev Attainment (%) | rev_attainment | sales_perfomance |
| Rev Variance ($) | rev_variance | sales_perfomance |
| Visit Target | visit_target | sales_perfomance |
| Visits Made | visit_made | sales_perfomance |
| Visit Att (%) | visit_attainment | sales_perfomance |
| Call Target | call_target | sales_perfomance |
| Calls Made | calls_made | sales_perfomance |
| Call Att (%) | call_attainment | sales_perfomance |
| Quote Target | quote_target | sales_perfomance |
| Quotes Made | quotes_made | sales_perfomance |
| Quote Att (%) | quote_attainment | sales_perfomance |
| Pipeline ($) | pipeline_value | sales_perfomance |
| Pipeline Cov (x) | pipeline_coverage | sales_perfomance |
| YTD Actual ($) | ytd_actual | sales_perfomance |
| Status | status | sales_perfomance |

### Entity Relationship

```
regions
  └── sales_managers (region_id → regions.id)
        └── sales_reps (manager_id → sales_managers.id)
              └── sales_perfomance (rep_id → sales_reps.id)
```

---

## Power BI Setup

### Option A — Flat view (recommended for quick reports)

Connect Power BI to PostgreSQL and import the view:

```
Database:  _sales_analysis
Object:   vw_powerbi_sales_performance
```

This view joins all four tables and exposes every column with human-readable names (region, sales_manager, sales_rep, etc.).

### Option B — Star model (recommended for complex dashboards)

Import all four tables and create relationships in Power BI:

| From | To |
|---|---|
| `sales_perfomance.rep_id` | `sales_reps.id` |
| `sales_reps.manager_id` | `sales_managers.id` |
| `sales_managers.region_id` | `regions.id` |

### Suggested measures (DAX)

```dax
Revenue Attainment % = AVERAGE(sales_perfomance[rev_attainment])

Revenue vs Target = 
    DIVIDE(SUM(sales_perfomance[rev_actual]), SUM(sales_perfomance[rev_target]), 0)

YTD Revenue = SUM(sales_perfomance[ytd_actual])

Pipeline Coverage = AVERAGE(sales_perfomance[pipeline_coverage])

On Track % = 
    DIVIDE(
        COUNTROWS(FILTER(sales_perfomance, SEARCH("On Track", sales_perfomance[status], 1, 0) > 0)),
        COUNTROWS(sales_perfomance),
        0
    )
```

### Suggested visuals

| Visual | Fields |
|---|---|
| KPI Card | Revenue Attainment %, YTD Revenue |
| Bar Chart | Revenue Actual vs Target by Region |
| Line Chart | Monthly Revenue Actual by Sales Rep |
| Matrix | Sales Manager → Rep → Attainment % |
| Donut Chart | Status breakdown (On Track / At Risk / Off Track) |
| Scatter Plot | Pipeline Value vs Revenue Actual |

---

## Pipeline Behaviour

- **Full reload** — every run truncates all four tables and reloads from source, preserving referential integrity.
- **Auto schema creation** — tables are created with `CREATE TABLE IF NOT EXISTS` on every run.
- **Auto database creation** — if ` _sales_analysis` does not exist, the pipeline creates it automatically via the bootstrap database.
- **Special characters in passwords** — handled safely via SQLAlchemy `URL.create()`.

---

## Troubleshooting

| Error | Cause | Fix |
|---|---|---|
| `password authentication failed` | Wrong `PG_PASSWORD` | Set the correct `$env:PG_PASSWORD` |
| `could not connect to server` | PostgreSQL not running or wrong host/port | Start PostgreSQL; verify `PG_HOST` and `PG_PORT` |
| `FileNotFoundError` | Excel file not found | Ensure `sales_performance_v2.xlsx` is in the same folder as the script |
| `Missing required columns` | Excel column names changed | Check that source column headers match expected names |
