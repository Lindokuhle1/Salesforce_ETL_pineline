import argparse
import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from sales_report import create_db_engine
from sales_report import load_config


logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
LOGGER = logging.getLogger("powerbi_export")


EXPORT_QUERIES: dict[str, str] = {
    "sales_detail": """
        SELECT
            performance_id,
            region,
            sales_manager,
            sales_rep,
            month,
            rev_target,
            rev_actual,
            rev_attainment,
            rev_variance,
            visit_target,
            visit_made,
            visit_attainment,
            call_target,
            calls_made,
            call_attainment,
            quote_target,
            quotes_made,
            quote_attainment,
            pipeline_value,
            pipeline_coverage,
            ytd_actual,
            status
        FROM vw_powerbi_sales_performance
        ORDER BY region, sales_manager, sales_rep, month;
    """,
    "kpi_by_region": """
        SELECT
            region,
            COUNT(*) AS records,
            SUM(rev_target) AS total_rev_target,
            SUM(rev_actual) AS total_rev_actual,
            ROUND(AVG(rev_attainment)::numeric, 4) AS avg_rev_attainment,
            ROUND(AVG(pipeline_coverage)::numeric, 4) AS avg_pipeline_coverage,
            SUM(ytd_actual) AS total_ytd_actual
        FROM vw_powerbi_sales_performance
        GROUP BY region
        ORDER BY region;
    """,
    "kpi_by_manager": """
        SELECT
            region,
            sales_manager,
            COUNT(*) AS records,
            SUM(rev_target) AS total_rev_target,
            SUM(rev_actual) AS total_rev_actual,
            ROUND(AVG(rev_attainment)::numeric, 4) AS avg_rev_attainment,
            ROUND(AVG(pipeline_coverage)::numeric, 4) AS avg_pipeline_coverage,
            SUM(ytd_actual) AS total_ytd_actual
        FROM vw_powerbi_sales_performance
        GROUP BY region, sales_manager
        ORDER BY region, sales_manager;
    """,
    "kpi_by_month": """
        SELECT
            month,
            COUNT(*) AS records,
            SUM(rev_target) AS total_rev_target,
            SUM(rev_actual) AS total_rev_actual,
            ROUND(AVG(rev_attainment)::numeric, 4) AS avg_rev_attainment,
            ROUND(AVG(pipeline_coverage)::numeric, 4) AS avg_pipeline_coverage,
            SUM(ytd_actual) AS total_ytd_actual
        FROM vw_powerbi_sales_performance
        GROUP BY month
        ORDER BY month;
    """,
}


def write_dataframe(df: pd.DataFrame, output_path: Path, export_format: str) -> None:
    if export_format == "csv":
        df.to_csv(output_path, index=False)
        return
    if export_format == "parquet":
        df.to_parquet(output_path, index=False)
        return
    raise ValueError(f"Unsupported export format: {export_format}")


def export_powerbi_datasets(output_dir: Path, export_format: str) -> int:
    config = load_config()
    engine = create_db_engine(config)
    output_dir.mkdir(parents=True, exist_ok=True)

    extension = "csv" if export_format == "csv" else "parquet"
    exported_files: list[Path] = []

    try:
        with engine.connect() as conn:
            for dataset_name, sql in EXPORT_QUERIES.items():
                df = pd.read_sql(text(sql), conn)
                output_path = output_dir / f"{dataset_name}.{extension}"
                write_dataframe(df, output_path, export_format)
                exported_files.append(output_path)
                LOGGER.info("Exported %s rows to %s", len(df), output_path)
    except OperationalError as exc:
        LOGGER.error(
            "Database connection failed. Verify PG_HOST/PG_PORT/PG_DATABASE/PG_USER/PG_PASSWORD and run sales_report.py first."
        )
        LOGGER.error("Details: %s", exc)
        return 2
    except Exception as exc:
        LOGGER.error("Export failed: %s", exc)
        return 1

    metadata_path = output_dir / "export_metadata.txt"
    metadata_lines = [
        f"exported_at_utc={datetime.now(timezone.utc).isoformat()}",
        f"database={config.pg_database}",
        f"format={export_format}",
    ]
    metadata_lines.extend(f"file={path.name}" for path in exported_files)
    metadata_path.write_text("\n".join(metadata_lines) + "\n", encoding="utf-8")
    LOGGER.info("Wrote export metadata to %s", metadata_path)

    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Export Power BI-ready datasets from PostgreSQL.")
    parser.add_argument(
        "--output-dir",
        default="powerbi_exports",
        help="Output folder for exported datasets (default: powerbi_exports)",
    )
    parser.add_argument(
        "--format",
        choices=["csv", "parquet"],
        default="csv",
        help="Export file format (default: csv)",
    )
    args = parser.parse_args()

    return export_powerbi_datasets(Path(args.output_dir).resolve(), args.format)


if __name__ == "__main__":
    raise SystemExit(main())