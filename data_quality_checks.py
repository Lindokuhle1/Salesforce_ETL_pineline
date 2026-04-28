import argparse
import json
import logging
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from sales_report import clean_data
from sales_report import load_config
from sales_report import read_excel_with_retry


logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
LOGGER = logging.getLogger("data_quality")


@dataclass
class CheckResult:
    name: str
    passed: bool
    details: str


def normalize_status(value: object) -> str:
    text = str(value).strip().lower()
    cleaned = "".join(ch for ch in text if ch.isalpha() or ch.isspace())
    return " ".join(cleaned.split())


def run_checks(df: pd.DataFrame) -> list[CheckResult]:
    results: list[CheckResult] = []

    key_cols = ["region", "sales_manager", "sales_rep", "month", "status"]
    missing_keys = df[key_cols].isna().sum().to_dict()
    missing_key_total = int(sum(missing_keys.values()))
    results.append(
        CheckResult(
            name="non_null_keys",
            passed=missing_key_total == 0,
            details=f"Missing values across key columns: {missing_keys}",
        )
    )

    valid_months = {
        "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec",
        "january", "february", "march", "april", "june", "july", "august", "september", "october", "november", "december",
    }
    month_tokens = df["month"].astype(str).str.strip().str.lower()
    invalid_months = sorted(df.loc[~month_tokens.isin(valid_months), "month"].astype(str).unique().tolist())
    results.append(
        CheckResult(
            name="valid_month_values",
            passed=len(invalid_months) == 0,
            details=f"Invalid month values: {invalid_months}",
        )
    )

    numeric_non_negative_cols = [
        "rev_target",
        "rev_actual",
        "visit_target",
        "visits_made",
        "call_target",
        "calls_made",
        "quote_target",
        "quotes_made",
        "pipeline_value",
        "pipeline_coverage",
        "ytd_actual",
    ]
    negatives = {
        col: int((pd.to_numeric(df[col], errors="coerce") < 0).sum())
        for col in numeric_non_negative_cols
    }
    negative_total = int(sum(negatives.values()))
    results.append(
        CheckResult(
            name="non_negative_metrics",
            passed=negative_total == 0,
            details=f"Negative values by column: {negatives}",
        )
    )

    normalized_status = df["status"].map(normalize_status)
    valid_status_patterns = {"on track", "at risk", "off track"}
    invalid_status = sorted(
        df.loc[~normalized_status.isin(valid_status_patterns), "status"].astype(str).unique().tolist()
    )
    results.append(
        CheckResult(
            name="valid_status_values",
            passed=len(invalid_status) == 0,
            details=f"Invalid status values: {invalid_status}",
        )
    )

    duplicate_keys = df.duplicated(subset=["sales_rep", "month"], keep=False)
    duplicate_count = int(duplicate_keys.sum())
    results.append(
        CheckResult(
            name="unique_sales_rep_month",
            passed=duplicate_count == 0,
            details=f"Duplicate sales_rep+month rows: {duplicate_count}",
        )
    )

    return results


def write_report(path: Path, row_count: int, checks: list[CheckResult]) -> None:
    payload = {
        "row_count": row_count,
        "passed": all(check.passed for check in checks),
        "checks": [
            {"name": c.name, "passed": c.passed, "details": c.details}
            for c in checks
        ],
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Run data-quality checks for sales source data.")
    parser.add_argument(
        "--report",
        default="dq_report.json",
        help="Output report path (default: dq_report.json)",
    )
    args = parser.parse_args()

    config = load_config()
    LOGGER.info("Running data-quality checks for: %s", config.input_file)

    if not config.input_file.exists():
        LOGGER.error("Input file not found: %s", config.input_file)
        return 2

    raw_df = read_excel_with_retry(config.input_file)
    cleaned_df = clean_data(raw_df)
    checks = run_checks(cleaned_df)
    passed = all(check.passed for check in checks)

    for check in checks:
        status = "PASS" if check.passed else "FAIL"
        LOGGER.info("[%s] %s | %s", status, check.name, check.details)

    report_path = Path(args.report).resolve()
    write_report(report_path, row_count=len(cleaned_df), checks=checks)
    LOGGER.info("Wrote quality report: %s", report_path)

    if not passed:
        LOGGER.error("Data-quality checks failed.")
        return 1

    LOGGER.info("All data-quality checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())