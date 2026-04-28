# Balancell Interview Prep Notes
**Interview Date:** 28 April 2026  
**Task:** Sales Performance ETL Pipeline + Power BI Report

---

## 1. Walk Me Through What You Built

> "Tell us about the solution you delivered."

**Your answer framework:**
1. **Extracted** raw KPI data from Excel (480 records, 21 columns, 40 sales reps across 5 regions)
2. **Transformed** it into a normalized relational model with proper foreign keys
3. **Loaded** it into PostgreSQL (`balancell_sales_analysis`) — 4 tables + 1 Power BI view
4. **Delivered** a flat reporting view (`vw_powerbi_sales_performance`) ready to connect directly in Power BI

**Key decisions to mention:**
- Used SQLAlchemy `URL.create()` instead of raw connection strings — safer for special characters in passwords
- Built a full reload strategy (TRUNCATE → reload) to keep the pipeline idempotent
- Auto-creates the database and tables if they don't exist — no manual setup required
- Environment variables for all credentials — no hardcoded secrets in the codebase

---

## 2. Technical Questions — Python & ETL

### Why Python for ETL?
- Widely used in data engineering; excellent library ecosystem (pandas, SQLAlchemy)
- Easy to schedule (cron, Windows Task Scheduler, Airflow)
- Readable and maintainable — a non-data-engineer can understand what the pipeline does

### What does pandas do in your pipeline?
- Reads the Excel file into a DataFrame
- Normalizes column names (strips spaces, lowercases, replaces spaces with underscores)
- Maps source aliases to canonical column names (e.g. `Pipeline ($)` → `pipeline_value`)
- Cleans types — ensures numeric columns are numeric, text columns are stripped
- Removes fully empty rows and duplicates

### What is SQLAlchemy and why use it?
- An ORM and database abstraction layer for Python
- Allows writing database-agnostic code — swap PostgreSQL for another DB with minimal changes
- `URL.create()` safely encodes special characters in passwords
- `engine.begin()` provides automatic transaction commit/rollback

### What does `if_exists="append"` mean vs `"replace"`?
- `replace` drops and recreates the table — loses your DDL, indexes, and foreign keys
- `append` adds rows to an existing table — we use this after manually truncating so foreign keys are preserved

### What is a TRUNCATE CASCADE?
- Deletes all rows from the table and any tables that reference it via foreign keys
- `RESTART IDENTITY` resets the auto-increment serial counters back to 1
- Ensures a clean slate on every pipeline run without dropping table structure

### How would you make this pipeline incremental?
- Add a `loaded_at TIMESTAMP` column to track when each record was inserted
- Compare `month` + `rep_id` as a natural composite key
- Use `INSERT ... ON CONFLICT DO UPDATE` (upsert) instead of truncate + reload
- Track the last loaded month and only process new records

---

## 3. Technical Questions — PostgreSQL

### Why normalize into 4 tables instead of one flat table?
- **No data redundancy** — region name is stored once, not repeated in every row
- **Data integrity** — foreign keys prevent orphaned records (a rep can't exist without a manager)
- **Performance** — smaller fact table scans faster; joins are indexed on integer keys
- **Maintainability** — renaming a region updates one row, not 480

### Explain your schema design
```
regions (5 rows)         ← master list of sales territories
  └── sales_managers (5) ← each manager belongs to one region
        └── sales_reps (40) ← each rep reports to one manager
              └── sales_perfomance (480) ← monthly KPI rows per rep
```

### What is a SERIAL PRIMARY KEY?
- PostgreSQL auto-incrementing integer column
- `SERIAL` is shorthand for `INTEGER DEFAULT nextval('sequence')`
- Guarantees a unique surrogate key for every row

### What is a foreign key REFERENCE?
- A constraint that enforces relational integrity
- `rep_id INTEGER REFERENCES sales_reps(id)` means every `rep_id` in `sales_perfomance` must exist in `sales_reps.id`
- Prevents loading performance rows for reps that don't exist

### Why did you create a view (`vw_powerbi_sales_performance`)?
- Power BI users don't need to understand foreign keys and joins
- The view exposes all human-readable columns in one place (region, manager, rep, month, all KPIs)
- Changing the underlying tables doesn't break the Power BI report — only the view needs updating

---

## 4. Technical Questions — Power BI

### How would you connect Power BI to PostgreSQL?
1. Home → Get Data → PostgreSQL database
2. Enter server: `127.0.0.1` and database: `balancell_sales_analysis`
3. Select `vw_powerbi_sales_performance` or individual tables
4. Load or Transform as needed

### What is the difference between Import and DirectQuery?
| | Import | DirectQuery |
|---|---|---|
| Data stored in | Power BI file | PostgreSQL |
| Refresh | Scheduled/manual | Real-time |
| Performance | Fast | Depends on DB |
| Best for | This use case (480 rows) | Large live datasets |

### What visuals would you use for this data?
- **KPI Cards** — Revenue Attainment %, YTD vs Target
- **Bar chart** — Rev Actual vs Target by Region or Manager
- **Line chart** — Monthly revenue trend per rep
- **Matrix** — Manager → Rep → Attainment % (drill-down)
- **Donut/Pie** — Status distribution (On Track / At Risk / Off Track)
- **Scatter plot** — Pipeline Value vs Revenue Actual (identify underperforming reps)
- **Conditional formatting** — Red/Amber/Green on attainment % columns

### What DAX measure would you write for revenue attainment?
```dax
Revenue Attainment % = 
    DIVIDE(SUM(sales_perfomance[rev_actual]), SUM(sales_perfomance[rev_target]), 0)
```

### How would you show only reps who are Off Track?
- Use a slicer on the `status` column
- Or write a measure: `CALCULATE([Revenue Attainment %], sales_perfomance[status] = "🔴 Off Track")`

---

## 5. Business / Analytical Questions

### What insights did you find in the data?
- Dataset covers **40 sales reps** across **5 regions** over **12 months**
- **3 performance statuses**: On Track ✅, At Risk ⚠️, Off Track 🔴
- Pipeline coverage (`pipeline_cov`) indicates future revenue health — reps with low pipeline are at risk next quarter
- Call and visit attainment often exceed 1.0 (>100%) but revenue attainment can still be below target — suggests activity ≠ revenue conversion
- YTD actual allows comparison of cumulative performance regardless of which month you're in

### What is pipeline coverage and why does it matter?
- Ratio of pipeline value to revenue target: `pipeline_value / rev_target`
- A coverage ratio of 3x means there's 3x more opportunity in the pipeline than the target
- Low pipeline coverage (< 2x) is an early warning that future months will miss target

### What does a rep being "At Risk" likely mean?
- Revenue attainment between ~0.85 and ~1.0 — close to target but not there yet
- May still recover by month end if pipeline is strong
- Manager should prioritize coaching these reps

### How would you define a "top performer"?
- Rev attainment > 1.0 AND pipeline coverage > 3x AND status = On Track
- Consistently in the top quartile of `ytd_actual` across months

---

## 6. Behavioral / Situational Questions

### "You had no brief — how did you decide what to build?"
- Started with the data: understood the structure (KPIs by rep, month, region)
- Asked: what does a sales manager need to know? → Who is on track, who isn't, why
- Built a normalized database because the data had clear relational structure
- Built a flat view because Power BI users need simplicity
- Prioritized correctness (FK integrity, clean types) over complexity

### "Why PostgreSQL and not Excel or a flat file?"
- PostgreSQL enforces data integrity (foreign keys, types, uniqueness)
- Scales — 480 rows today, 50,000 rows next year, same pipeline
- Power BI connects natively to PostgreSQL with scheduled refresh
- Version-controlled DDL means the schema is documented and reproducible

### "What would you improve if you had more time?"
- Add incremental loading (upsert instead of full reload)
- Add data quality checks (e.g. flag rows where `rev_actual < 0`)
- Add a pipeline run log table to track when data was last loaded
- Parameterize the month filter so you can reload a single month
- Deploy on a scheduled task or Apache Airflow

### "What was the hardest part of this task?"
- Mapping source column aliases reliably (e.g. `Rev Attainment (%)` with special characters)
- Building foreign keys in Python — assigning surrogate IDs to dimensions and joining them back to the fact table without key collisions
- Handling reps who appear under multiple managers across regions (resolved by keeping the first mapping)

### "How did you ensure data quality?"
- Stripped whitespace from all text columns
- Cast all numeric columns explicitly with `errors="coerce"` (bad values become NaN instead of crashing)
- Dropped fully empty rows and duplicates
- Used `UNIQUE NOT NULL` constraints on dimension tables to prevent duplicates at the database level

---

## 7. Quick Facts to Know Off the Top of Your Head

| Fact | Value |
|---|---|
| Source file | sales_performance_v2.xlsx |
| Total rows | 480 |
| Columns | 21 |
| Regions | 5 |
| Sales managers | 5 |
| Sales reps | 40 |
| Months covered | 12 (Jan–Dec) |
| PostgreSQL database | balancell_sales_analysis |
| Tables | regions, sales_managers, sales_reps, sales_perfomance |
| Power BI object | vw_powerbi_sales_performance |
| Python libraries | pandas, sqlalchemy, psycopg2-binary, openpyxl |

---

## 8. Questions to Ask Them

- What does the sales team currently use to track performance — Excel, a BI tool, a CRM?
- How often does this data need to be refreshed — daily, weekly, monthly?
- Are there other data sources you'd want to bring into this pipeline (e.g. CRM, invoicing)?
- What does success look like for this role in the first 90 days?
- Who are the primary consumers of the reports — sales managers, executives, both?
