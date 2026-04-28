import argparse
import json
import logging
import shutil
import time
from dataclasses import asdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from data_quality_checks import run_checks
from data_quality_checks import write_report
from manager_report import generate_manager_reports
from powerbi_export import export_powerbi_datasets
from sales_report import clean_data
from sales_report import load_config
from sales_report import read_excel_with_retry
from sales_report import run_pipeline


logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
LOGGER = logging.getLogger("orchestration")


@dataclass
class StageResult:
    name: str
    status: str
    started_at_utc: str
    ended_at_utc: str
    duration_seconds: float
    message: str


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def run_data_quality_stage(report_path: Path) -> tuple[bool, str]:
    config = load_config()
    if not config.input_file.exists():
        return False, f"Input file not found: {config.input_file}"

    raw_df = read_excel_with_retry(config.input_file)
    cleaned_df = clean_data(raw_df)
    checks = run_checks(cleaned_df)
    write_report(report_path, row_count=len(cleaned_df), checks=checks)

    failed_checks = [check.name for check in checks if not check.passed]
    if failed_checks:
        return False, f"Data quality failed: {failed_checks}. Report: {report_path}"

    return True, f"Data quality passed. Report: {report_path}"


def run_etl_stage() -> tuple[bool, str]:
    try:
        run_pipeline()
        return True, "ETL pipeline completed successfully."
    except Exception as exc:
        return False, f"ETL failed: {exc}"


def run_export_stage(output_dir: Path, export_format: str) -> tuple[bool, str]:
    code = export_powerbi_datasets(output_dir=output_dir, export_format=export_format)
    if code == 0:
        return True, f"Power BI export completed. Output: {output_dir}"
    return False, f"Power BI export failed with exit code {code}."


def run_dashboard_sync_stage(output_dir: Path, dashboard_data_dir: Path, export_format: str) -> tuple[bool, str]:
    extension = "csv" if export_format == "csv" else "parquet"
    required_files = [
        f"sales_detail.{extension}",
        f"kpi_by_region.{extension}",
        f"kpi_by_manager.{extension}",
        f"kpi_by_month.{extension}",
    ]

    missing = [name for name in required_files if not (output_dir / name).exists()]
    if missing:
        return False, f"Dashboard sync failed. Missing export files in {output_dir}: {missing}"

    dashboard_data_dir.mkdir(parents=True, exist_ok=True)

    copied_files: list[str] = []
    for file_name in required_files:
        source_path = output_dir / file_name
        target_path = dashboard_data_dir / file_name
        shutil.copy2(source_path, target_path)
        copied_files.append(file_name)

    metadata_path = output_dir / "export_metadata.txt"
    if metadata_path.exists():
        shutil.copy2(metadata_path, dashboard_data_dir / metadata_path.name)
        copied_files.append(metadata_path.name)

    return True, f"Dashboard data synced to {dashboard_data_dir}. Files: {copied_files}"


def run_manager_report_stage(output_dir: Path, reports_dir: Path, export_format: str) -> tuple[bool, str]:
    try:
        excel_path, html_path = generate_manager_reports(
            output_dir=output_dir,
            reports_dir=reports_dir,
            export_format=export_format,
        )
        return True, f"Manager reports generated: Excel={excel_path}, HTML={html_path}"
    except Exception as exc:
        return False, f"Manager report generation failed: {exc}"


def execute_stage(name: str, runner) -> StageResult:
    started = utc_now_iso()
    t0 = time.perf_counter()
    ok, message = runner()
    duration = round(time.perf_counter() - t0, 3)
    ended = utc_now_iso()

    status = "success" if ok else "failed"
    if ok:
        LOGGER.info("[%s] %s", name, message)
    else:
        LOGGER.error("[%s] %s", name, message)

    return StageResult(
        name=name,
        status=status,
        started_at_utc=started,
        ended_at_utc=ended,
        duration_seconds=duration,
        message=message,
    )


def write_run_summary(path: Path, stage_results: list[StageResult]) -> None:
    payload = {
        "run_id": datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"),
        "generated_at_utc": utc_now_iso(),
        "overall_status": "success" if all(stage.status == "success" for stage in stage_results) else "failed",
        "stages": [asdict(stage) for stage in stage_results],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Run full orchestration: quality checks -> ETL -> Power BI export.")
    parser.add_argument(
        "--skip-dq",
        action="store_true",
        help="Skip data quality stage.",
    )
    parser.add_argument(
        "--skip-export",
        action="store_true",
        help="Skip Power BI export stage.",
    )
    parser.add_argument(
        "--continue-on-dq-fail",
        action="store_true",
        help="Continue ETL and export even if data quality checks fail.",
    )
    parser.add_argument(
        "--dq-report",
        default="dq_report.json",
        help="Data quality report path (default: dq_report.json).",
    )
    parser.add_argument(
        "--output-dir",
        default="powerbi_exports",
        help="Power BI export output folder (default: powerbi_exports).",
    )
    parser.add_argument(
        "--format",
        choices=["csv", "parquet"],
        default="csv",
        help="Power BI export format (default: csv).",
    )
    parser.add_argument(
        "--summary",
        default="orchestration_runs/latest_run.json",
        help="Run summary output path (default: orchestration_runs/latest_run.json).",
    )
    parser.add_argument(
        "--skip-dashboard-sync",
        action="store_true",
        help="Skip syncing exported datasets into the React dashboard public/data folder.",
    )
    parser.add_argument(
        "--dashboard-data-dir",
        default="sales-dashboard/public/data",
        help="React dashboard data folder for auto-sync (default: sales-dashboard/public/data).",
    )
    parser.add_argument(
        "--skip-manager-report",
        action="store_true",
        help="Skip manager report generation stage (Excel + HTML).",
    )
    parser.add_argument(
        "--reports-dir",
        default="reports",
        help="Output folder for generated manager reports (default: reports).",
    )
    args = parser.parse_args()

    stage_results: list[StageResult] = []
    dq_failed = False

    if not args.skip_dq:
        dq_result = execute_stage("data_quality", lambda: run_data_quality_stage(Path(args.dq_report).resolve()))
        stage_results.append(dq_result)
        dq_failed = dq_result.status != "success"
        if dq_failed and not args.continue_on_dq_fail:
            write_run_summary(Path(args.summary).resolve(), stage_results)
            LOGGER.error("Orchestration stopped due to data quality failure.")
            return 1

    etl_result = execute_stage("etl", run_etl_stage)
    stage_results.append(etl_result)
    if etl_result.status != "success":
        write_run_summary(Path(args.summary).resolve(), stage_results)
        LOGGER.error("Orchestration stopped due to ETL failure.")
        return 1

    if not args.skip_export:
        export_result = execute_stage(
            "powerbi_export",
            lambda: run_export_stage(Path(args.output_dir).resolve(), args.format),
        )
        stage_results.append(export_result)
        if export_result.status != "success":
            write_run_summary(Path(args.summary).resolve(), stage_results)
            LOGGER.error("Orchestration completed with export failure.")
            return 1

    if not args.skip_dashboard_sync:
        sync_result = execute_stage(
            "dashboard_sync",
            lambda: run_dashboard_sync_stage(
                Path(args.output_dir).resolve(),
                Path(args.dashboard_data_dir).resolve(),
                args.format,
            ),
        )
        stage_results.append(sync_result)
        if sync_result.status != "success":
            write_run_summary(Path(args.summary).resolve(), stage_results)
            LOGGER.error("Orchestration completed with dashboard sync failure.")
            return 1

    if not args.skip_manager_report:
        report_result = execute_stage(
            "manager_report",
            lambda: run_manager_report_stage(
                Path(args.output_dir).resolve(),
                Path(args.reports_dir).resolve(),
                args.format,
            ),
        )
        stage_results.append(report_result)
        if report_result.status != "success":
            write_run_summary(Path(args.summary).resolve(), stage_results)
            LOGGER.error("Orchestration completed with manager report failure.")
            return 1

    write_run_summary(Path(args.summary).resolve(), stage_results)
    LOGGER.info("Orchestration completed successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())