import argparse
from datetime import datetime
from pathlib import Path
import shutil
import tempfile

import pandas as pd
from openpyxl.chart import BarChart
from openpyxl.chart import LineChart
from openpyxl.chart import Reference


def _read_export(path_base: Path, dataset: str, export_format: str) -> pd.DataFrame:
    extension = "csv" if export_format == "csv" else "parquet"
    file_path = path_base / f"{dataset}.{extension}"
    if not file_path.exists():
        raise FileNotFoundError(f"Missing export file: {file_path}")

    if export_format == "csv":
        return pd.read_csv(file_path)
    return pd.read_parquet(file_path)


def _safe_num(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(0)


def _month_order(month_series: pd.Series) -> list[str]:
    default_order = [
        "Jan",
        "Feb",
        "Mar",
        "Apr",
        "May",
        "Jun",
        "Jul",
        "Aug",
        "Sep",
        "Oct",
        "Nov",
        "Dec",
    ]

    values = [str(v) for v in month_series.dropna().unique()]
    by_default = [m for m in default_order if m in values]
    extras = sorted([m for m in values if m not in default_order])
    return by_default + extras


def _build_manager_summary(detail_df: pd.DataFrame) -> pd.DataFrame:
    grouped = (
        detail_df.groupby(["region", "sales_manager"], as_index=False)
        .agg(
            rev_target=("rev_target", "sum"),
            rev_actual=("rev_actual", "sum"),
            pipeline_value=("pipeline_value", "sum"),
            ytd_actual=("ytd_actual", "sum"),
            reps=("sales_rep", "nunique"),
            records=("performance_id", "count"),
        )
        .rename(columns={"sales_manager": "manager"})
    )
    grouped["attainment"] = grouped.apply(
        lambda r: (r["rev_actual"] / r["rev_target"]) if r["rev_target"] else 0,
        axis=1,
    )
    grouped["variance"] = grouped["rev_actual"] - grouped["rev_target"]
    grouped = grouped.sort_values(["region", "attainment"], ascending=[True, False]).reset_index(drop=True)
    return grouped


def _build_monthly_summary(detail_df: pd.DataFrame) -> pd.DataFrame:
    month_sort = _month_order(detail_df["month"])
    month_rank = {name: idx for idx, name in enumerate(month_sort)}

    grouped = (
        detail_df.groupby("month", as_index=False)
        .agg(
            rev_target=("rev_target", "sum"),
            rev_actual=("rev_actual", "sum"),
            pipeline_value=("pipeline_value", "sum"),
            records=("performance_id", "count"),
        )
        .assign(attainment=lambda df: df["rev_actual"] / df["rev_target"].replace({0: pd.NA}))
        .fillna({"attainment": 0})
    )

    grouped["_sort"] = grouped["month"].map(lambda v: month_rank.get(str(v), 999))
    grouped = grouped.sort_values("_sort").drop(columns=["_sort"]).reset_index(drop=True)
    return grouped


def _build_rep_summary(detail_df: pd.DataFrame) -> pd.DataFrame:
    grouped = (
        detail_df.groupby(["sales_manager", "sales_rep"], as_index=False)
        .agg(
            rev_target=("rev_target", "sum"),
            rev_actual=("rev_actual", "sum"),
            pipeline_value=("pipeline_value", "sum"),
            months=("month", "nunique"),
            latest_status=("status", "last"),
        )
        .rename(columns={"sales_manager": "manager", "sales_rep": "rep"})
    )
    grouped["attainment"] = grouped.apply(
        lambda r: (r["rev_actual"] / r["rev_target"]) if r["rev_target"] else 0,
        axis=1,
    )
    return grouped.sort_values(["manager", "attainment"], ascending=[True, False]).reset_index(drop=True)


def _svg_bar_chart(data: pd.DataFrame, label_col: str, value_col: str, title: str, currency: bool = False) -> str:
    width = 900
    height = 340
    pad_top = 44
    pad_left = 56
    pad_right = 26
    bar_area_height = 220
    labels_y = 292

    max_value = float(data[value_col].max()) if not data.empty else 1.0
    max_value = max(max_value, 1.0)
    usable_width = width - pad_left - pad_right
    bar_count = max(len(data), 1)
    bar_width = max(30, int((usable_width / bar_count) * 0.55))
    gap = (usable_width - (bar_width * bar_count)) / max(bar_count - 1, 1)

    bars = []
    labels = []
    values = []
    for idx, (_, row) in enumerate(data.iterrows()):
        value = float(row[value_col])
        label = str(row[label_col])
        h = (value / max_value) * bar_area_height
        x = pad_left + idx * (bar_width + gap)
        y = pad_top + (bar_area_height - h)
        bars.append(f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_width}" height="{h:.1f}" fill="#264653" rx="6"/>')
        labels.append(
            f'<text x="{x + bar_width / 2:.1f}" y="{labels_y}" text-anchor="middle" font-size="12" fill="#5f5d58">{label}</text>'
        )
        val_text = f"${value:,.0f}" if currency else f"{value * 100:.1f}%"
        values.append(
            f'<text x="{x + bar_width / 2:.1f}" y="{max(20, y - 6):.1f}" text-anchor="middle" font-size="11" fill="#1a1a1a">{val_text}</text>'
        )

    return (
        f'<svg viewBox="0 0 {width} {height}" width="100%" height="100%" role="img" aria-label="{title}">'
        f'<text x="22" y="26" font-size="16" font-weight="700" fill="#1a1a1a">{title}</text>'
        f'<line x1="{pad_left}" y1="{pad_top + bar_area_height}" x2="{width - pad_right}" y2="{pad_top + bar_area_height}" stroke="#cfc4b5"/>'
        f"{''.join(bars)}{''.join(values)}{''.join(labels)}"
        "</svg>"
    )


def _svg_line_chart(data: pd.DataFrame, x_col: str, y_col: str, title: str) -> str:
    width = 900
    height = 340
    pad_top = 44
    pad_left = 50
    pad_right = 24
    pad_bottom = 46

    plot_w = width - pad_left - pad_right
    plot_h = height - pad_top - pad_bottom
    max_value = float(data[y_col].max()) if not data.empty else 1.0
    max_value = max(max_value, 1.0)

    points = []
    labels = []
    for idx, (_, row) in enumerate(data.iterrows()):
        x = pad_left + (idx * plot_w / max(len(data) - 1, 1))
        y = pad_top + (plot_h - (float(row[y_col]) / max_value) * plot_h)
        points.append(f"{x:.1f},{y:.1f}")
        labels.append(
            f'<text x="{x:.1f}" y="{height - 18}" text-anchor="middle" font-size="12" fill="#5f5d58">{row[x_col]}</text>'
        )

    poly = " ".join(points)
    circles = "".join(
        [f'<circle cx="{p.split(",")[0]}" cy="{p.split(",")[1]}" r="3.8" fill="#2a9d8f"/>' for p in points]
    )

    return (
        f'<svg viewBox="0 0 {width} {height}" width="100%" height="100%" role="img" aria-label="{title}">'
        f'<text x="22" y="26" font-size="16" font-weight="700" fill="#1a1a1a">{title}</text>'
        f'<line x1="{pad_left}" y1="{pad_top + plot_h}" x2="{width - pad_right}" y2="{pad_top + plot_h}" stroke="#cfc4b5"/>'
        f'<polyline points="{poly}" fill="none" stroke="#2a9d8f" stroke-width="3"/>'
        f"{circles}{''.join(labels)}"
        "</svg>"
    )


def _html_table(df: pd.DataFrame, percent_cols: set[str], money_cols: set[str]) -> str:
    header_html = "".join(f"<th>{col.replace('_', ' ').title()}</th>" for col in df.columns)
    rows_html = []

    for _, row in df.iterrows():
        cells = []
        for col in df.columns:
            value = row[col]
            if col in percent_cols:
                cells.append(f"<td>{float(value) * 100:.1f}%</td>")
            elif col in money_cols:
                cells.append(f"<td>${float(value):,.0f}</td>")
            else:
                cells.append(f"<td>{value}</td>")
        rows_html.append("<tr>" + "".join(cells) + "</tr>")

    return (
        '<div class="table-wrap">'
        '<table>'
        f"<thead><tr>{header_html}</tr></thead>"
        f"<tbody>{''.join(rows_html)}</tbody>"
        "</table></div>"
    )


def _write_excel_report(
    output_file: Path,
    manager_summary: pd.DataFrame,
    monthly_summary: pd.DataFrame,
    rep_summary: pd.DataFrame,
    offtrack_reps: pd.DataFrame,
) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
        manager_summary.to_excel(writer, index=False, sheet_name="Manager Summary")
        monthly_summary.to_excel(writer, index=False, sheet_name="Monthly Trend")
        rep_summary.to_excel(writer, index=False, sheet_name="Rep Performance")
        offtrack_reps.to_excel(writer, index=False, sheet_name="At Risk Reps")

        workbook = writer.book
        ws_mgr = workbook["Manager Summary"]
        ws_month = workbook["Monthly Trend"]

        attainment_col_idx = manager_summary.columns.get_loc("attainment") + 1
        manager_col_idx = manager_summary.columns.get_loc("manager") + 1

        bar = BarChart()
        bar.title = "Manager Attainment"
        bar.y_axis.title = "Attainment"
        bar.x_axis.title = "Manager"
        bar.height = 8
        bar.width = 16

        data_ref = Reference(ws_mgr, min_col=attainment_col_idx + 1, min_row=1, max_row=len(manager_summary) + 1)
        cat_ref = Reference(ws_mgr, min_col=manager_col_idx + 1, min_row=2, max_row=len(manager_summary) + 1)
        bar.add_data(data_ref, titles_from_data=True)
        bar.set_categories(cat_ref)
        ws_mgr.add_chart(bar, "K2")

        month_target_idx = monthly_summary.columns.get_loc("rev_target") + 1
        month_actual_idx = monthly_summary.columns.get_loc("rev_actual") + 1

        line = LineChart()
        line.title = "Monthly Revenue Trend"
        line.y_axis.title = "Revenue"
        line.x_axis.title = "Month"
        line.height = 8
        line.width = 16

        target_ref = Reference(ws_month, min_col=month_target_idx + 1, min_row=1, max_row=len(monthly_summary) + 1)
        actual_ref = Reference(ws_month, min_col=month_actual_idx + 1, min_row=1, max_row=len(monthly_summary) + 1)
        month_cat_ref = Reference(ws_month, min_col=1, min_row=2, max_row=len(monthly_summary) + 1)
        line.add_data(target_ref, titles_from_data=True)
        line.add_data(actual_ref, titles_from_data=True)
        line.set_categories(month_cat_ref)
        ws_month.add_chart(line, "H2")

        for sheet_name in ["Manager Summary", "Monthly Trend", "Rep Performance", "At Risk Reps"]:
            ws = workbook[sheet_name]
            for col_cells in ws.columns:
                max_len = max(len(str(cell.value)) if cell.value is not None else 0 for cell in col_cells)
                ws.column_dimensions[col_cells[0].column_letter].width = min(max(max_len + 2, 12), 30)


def _write_html_report(
    output_file: Path,
    manager_summary: pd.DataFrame,
    monthly_summary: pd.DataFrame,
    rep_summary: pd.DataFrame,
    offtrack_reps: pd.DataFrame,
) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)

    top_managers = manager_summary.nlargest(8, "attainment")[["manager", "attainment"]].copy()
    monthly_chart_data = monthly_summary[["month", "rev_actual"]].copy()

    total_target = float(manager_summary["rev_target"].sum())
    total_actual = float(manager_summary["rev_actual"].sum())
    overall_attainment = (total_actual / total_target) if total_target else 0
    manager_count = int(manager_summary["manager"].nunique())
    risk_count = int(len(offtrack_reps))

    html = f"""
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Manager Performance Report</title>
  <style>
    body {{
      margin: 0;
      font-family: Segoe UI, Tahoma, sans-serif;
      background: #f6f1e8;
      color: #1a1a1a;
      padding: 20px;
    }}
    .container {{
      max-width: 1100px;
      margin: 0 auto;
    }}
    .hero {{
      background: #fffdf8;
      border: 1px solid #ddd3c2;
      border-radius: 16px;
      padding: 18px;
      margin-bottom: 14px;
    }}
    h1 {{ margin: 0 0 8px; font-size: 30px; }}
    .meta {{ color: #5f5d58; margin: 0; }}
    .kpis {{
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 10px;
      margin: 12px 0 14px;
    }}
    .kpi {{
      background: #fffdf8;
      border: 1px solid #ddd3c2;
      border-radius: 12px;
      padding: 12px;
    }}
    .kpi .label {{ font-size: 12px; color: #5f5d58; text-transform: uppercase; letter-spacing: 0.03em; }}
    .kpi .value {{ font-size: 24px; margin-top: 6px; font-weight: 700; }}
    .panel {{
      background: #fffdf8;
      border: 1px solid #ddd3c2;
      border-radius: 12px;
      padding: 10px;
      margin-bottom: 12px;
    }}
    .charts {{
      display: grid;
      grid-template-columns: 1fr;
      gap: 10px;
    }}
    table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
    th, td {{ border-bottom: 1px solid #ece3d6; text-align: left; padding: 8px 7px; }}
    th {{ text-transform: uppercase; font-size: 11px; color: #5f5d58; }}
    .table-wrap {{ overflow-x: auto; }}
    @media (max-width: 900px) {{
      .kpis {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
    }}
    @media (max-width: 560px) {{
      .kpis {{ grid-template-columns: 1fr; }}
      h1 {{ font-size: 24px; }}
    }}
  </style>
</head>
<body>
  <div class="container">
    <section class="hero">
      <h1>Manager Performance Report</h1>
      <p class="meta">Generated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Shareable HTML for email updates</p>
    </section>

    <section class="kpis">
      <div class="kpi">
        <div class="label">Overall Attainment</div>
        <div class="value">{overall_attainment * 100:.1f}%</div>
      </div>
      <div class="kpi">
        <div class="label">Revenue Target</div>
        <div class="value">${total_target:,.0f}</div>
      </div>
      <div class="kpi">
        <div class="label">Revenue Actual</div>
        <div class="value">${total_actual:,.0f}</div>
      </div>
      <div class="kpi">
        <div class="label">Managers Tracked</div>
        <div class="value">{manager_count}</div>
      </div>
    </section>

    <section class="charts">
      <article class="panel">
        {_svg_bar_chart(top_managers, 'manager', 'attainment', 'Top Manager Attainment', currency=False)}
      </article>
      <article class="panel">
        {_svg_line_chart(monthly_chart_data, 'month', 'rev_actual', 'Monthly Revenue Trend')}
      </article>
    </section>

    <section class="panel">
      <h3>Manager Summary</h3>
      {_html_table(
          manager_summary[["region", "manager", "attainment", "variance", "rev_actual", "pipeline_value", "reps"]].round(4),
          percent_cols={"attainment"},
          money_cols={"variance", "rev_actual", "pipeline_value"},
      )}
    </section>

    <section class="panel">
      <h3>Top Reps</h3>
      {_html_table(
          rep_summary.nlargest(12, "rev_actual")[["manager", "rep", "attainment", "rev_actual", "pipeline_value", "latest_status"]].round(4),
          percent_cols={"attainment"},
          money_cols={"rev_actual", "pipeline_value"},
      )}
    </section>

    <section class="panel">
      <h3>At Risk / Off Track Snapshot ({risk_count})</h3>
      {_html_table(
          offtrack_reps[["sales_manager", "sales_rep", "month", "status", "rev_attainment", "pipeline_value"]].head(20).round(4),
          percent_cols={"rev_attainment"},
          money_cols={"pipeline_value"},
      )}
    </section>
  </div>
</body>
</html>
"""

    output_file.write_text(html, encoding="utf-8")


def generate_manager_reports(output_dir: Path, reports_dir: Path, export_format: str = "csv") -> tuple[Path, Path]:
    detail_df = _read_export(output_dir, "sales_detail", export_format).copy()

    for col in ["rev_target", "rev_actual", "pipeline_value", "ytd_actual", "rev_attainment"]:
        detail_df[col] = _safe_num(detail_df[col])

    manager_summary = _build_manager_summary(detail_df)
    monthly_summary = _build_monthly_summary(detail_df)
    rep_summary = _build_rep_summary(detail_df)
    offtrack_reps = detail_df[
        detail_df["status"].astype(str).str.contains("Off Track|At Risk", case=False, regex=True)
    ].copy()

    reports_dir.mkdir(parents=True, exist_ok=True)
    excel_path = reports_dir / "manager_performance_report.xlsx"
    html_path = reports_dir / "manager_performance_report.html"

    # Write Excel report to temporary file first to avoid file locking issues
    with tempfile.TemporaryDirectory() as temp_dir:
      temp_excel = Path(temp_dir) / "manager_performance_report.xlsx"
      _write_excel_report(
        output_file=temp_excel,
        manager_summary=manager_summary,
        monthly_summary=monthly_summary,
        rep_summary=rep_summary,
        offtrack_reps=offtrack_reps,
      )
      # Move temp file to final location, overwriting if exists
      try:
        if excel_path.exists():
          excel_path.unlink()
        shutil.copy2(temp_excel, excel_path)
      except Exception as e:
        # File is locked, save to timestamped backup in reports dir
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        excel_path = reports_dir / f"manager_performance_report_{timestamp}.xlsx"
        shutil.copy2(temp_excel, excel_path)
        print(f"Note: File was locked, saved as: {excel_path.name}")

    _write_html_report(
        output_file=html_path,
        manager_summary=manager_summary,
        monthly_summary=monthly_summary,
        rep_summary=rep_summary,
        offtrack_reps=offtrack_reps,
    )

    return excel_path, html_path


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate manager performance reports (Excel + HTML) from exported datasets.")
    parser.add_argument("--output-dir", default="powerbi_exports", help="Folder containing exported datasets.")
    parser.add_argument("--reports-dir", default="reports", help="Folder to store generated reports.")
    parser.add_argument("--format", choices=["csv", "parquet"], default="csv", help="Dataset format in output-dir.")
    args = parser.parse_args()

    excel_path, html_path = generate_manager_reports(
        output_dir=Path(args.output_dir).resolve(),
        reports_dir=Path(args.reports_dir).resolve(),
        export_format=args.format,
    )
    print(f"Generated Excel report: {excel_path}")
    print(f"Generated HTML report: {html_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())