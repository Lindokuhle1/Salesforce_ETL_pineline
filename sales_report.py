import logging
import os
import re
import shutil
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.engine import URL
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError


logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
LOGGER = logging.getLogger("sales_etl")


@dataclass
class PipelineConfig:
    input_file: Path
    pg_host: str
    pg_port: int
    pg_database: str
    pg_user: str
    pg_password: str
    pg_bootstrap_db: str

    @property
    def connection_url(self) -> URL:
        return URL.create(
            drivername="postgresql+psycopg2",
            username=self.pg_user,
            password=self.pg_password,
            host=self.pg_host,
            port=self.pg_port,
            database=self.pg_database,
        )


def load_config() -> PipelineConfig:
    base_dir = Path(__file__).resolve().parent
    return PipelineConfig(
        input_file=base_dir / os.getenv("INPUT_FILE", "sales_performance_v2.xlsx"),
        pg_host=os.getenv("PG_HOST", "127.0.0.1"),
        pg_port=int(os.getenv("PG_PORT", "5432")),
        pg_database=os.getenv("PG_DATABASE", "balancell_sales_analysis"),
        pg_user=os.getenv("PG_USER", "postgres"),
        pg_password=os.getenv("PG_PASSWORD", "Phambuka1@"),
        pg_bootstrap_db=os.getenv("PG_BOOTSTRAP_DB", "postgres"),
    )


def read_excel_with_retry(input_file: Path, attempts: int = 5, delay_seconds: float = 1.5) -> pd.DataFrame:
    last_error: Exception | None = None

    for attempt in range(1, attempts + 1):
        try:
            return pd.read_excel(input_file)
        except PermissionError as exc:
            last_error = exc
            LOGGER.warning(
                "Excel file is locked or unavailable (attempt %s/%s). Retrying in %.1fs...",
                attempt,
                attempts,
                delay_seconds,
            )
            if attempt < attempts:
                time.sleep(delay_seconds)

    # If direct access fails repeatedly, try reading from a temporary copy.
    temp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(suffix=input_file.suffix, delete=False) as tmp:
            temp_path = Path(tmp.name)

        shutil.copy2(input_file, temp_path)
        LOGGER.info("Reading source from temporary copy: %s", temp_path)
        return pd.read_excel(temp_path)
    except PermissionError as exc:
        last_error = exc
    finally:
        if temp_path and temp_path.exists():
            try:
                temp_path.unlink()
            except OSError:
                LOGGER.warning("Could not remove temporary file: %s", temp_path)

    raise RuntimeError(
        "Cannot access Excel file due to permission lock. Close the file in Excel, pause OneDrive sync briefly, "
        "and run the pipeline again."
    ) from last_error


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = (
        out.columns.astype(str)
        .str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
        .str.replace("-", "_", regex=False)
    )
    return out


def coalesce_column(df: pd.DataFrame, preferred_name: str, candidates: list[str]) -> pd.DataFrame:
    available = [col for col in candidates if col in df.columns]
    if not available:
        return df

    df = df.copy()
    df[preferred_name] = df[available].bfill(axis=1).iloc[:, 0]
    drop_cols = [col for col in available if col != preferred_name]
    if drop_cols:
        df = df.drop(columns=drop_cols)
    return df


def standardize_schema(df: pd.DataFrame) -> pd.DataFrame:
    out = normalize_columns(df)

    alias_map = {
        "region": ["region"],
        "sales_manager": ["sales_manager", "manager"],
        "sales_rep": ["sales_rep", "salesperson", "sales_person", "rep"],
        "month": ["month"],
        "rev_target": ["rev_target", "rev_target_($)"],
        "rev_actual": ["rev_actual", "rev_actual_($)"],
        "rev_attainment": ["rev_attainment", "rev_attainment_(%)", "rev_attainment_pct"],
        "rev_variance": ["rev_variance", "rev_variance_($)"],
        "visit_target": ["visit_target"],
        "visits_made": ["visits_made", "visit_made"],
        "visit_attainment": ["visit_attainment", "visit_att_(%)", "visit_att_pct"],
        "call_target": ["call_target"],
        "calls_made": ["calls_made"],
        "call_attainment": ["call_attainment", "call_att_(%)", "call_att_pct"],
        "quote_target": ["quote_target"],
        "quotes_made": ["quotes_made"],
        "quote_attainment": ["quote_attainment", "quote_att_(%)", "quote_att_pct"],
        "pipeline_value": ["pipeline_value", "pipeline_($)", "pipeline_amount"],
        "pipeline_coverage": ["pipeline_coverage", "pipeline_cov_(x)", "pipeline_cov_x"],
        "ytd_actual": ["ytd_actual", "ytd_actual_($)"],
        "status": ["status"],
    }

    for canonical_name, aliases in alias_map.items():
        out = coalesce_column(out, canonical_name, aliases)

    return out


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    out = standardize_schema(df)

    required_cols = [
        "region",
        "sales_manager",
        "sales_rep",
        "month",
        "rev_target",
        "rev_actual",
        "rev_attainment",
        "rev_variance",
        "visit_target",
        "visits_made",
        "visit_attainment",
        "call_target",
        "calls_made",
        "call_attainment",
        "quote_target",
        "quotes_made",
        "quote_attainment",
        "pipeline_value",
        "pipeline_coverage",
        "ytd_actual",
        "status",
    ]

    missing = [col for col in required_cols if col not in out.columns]
    if missing:
        raise ValueError(f"Missing required columns in source file: {missing}")

    numeric_cols = [
        "rev_target",
        "rev_actual",
        "rev_attainment",
        "rev_variance",
        "visit_target",
        "visits_made",
        "visit_attainment",
        "call_target",
        "calls_made",
        "call_attainment",
        "quote_target",
        "quotes_made",
        "quote_attainment",
        "pipeline_value",
        "pipeline_coverage",
        "ytd_actual",
    ]
    for col in numeric_cols:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    text_cols = ["region", "sales_manager", "sales_rep", "month", "status"]
    for col in text_cols:
        out[col] = out[col].astype(str).str.strip()

    out = out.dropna(how="all")
    out = out.drop_duplicates()
    return out


def to_int_series(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(0).round().astype(int)


def ratio_to_int_percent(series: pd.Series) -> pd.Series:
    cleaned = pd.to_numeric(series, errors="coerce").fillna(0)
    if cleaned.abs().max() <= 2:
        cleaned = cleaned * 100
    return cleaned.round().astype(int)


def build_relational_model(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    work_df = df.copy()

    regions = (
        work_df[["region"]]
        .drop_duplicates()
        .sort_values("region")
        .reset_index(drop=True)
    )
    regions["id"] = regions.index + 1
    regions = regions.rename(columns={"region": "name"})[["id", "name"]]

    managers = (
        work_df[["sales_manager", "region"]]
        .drop_duplicates()
        .merge(regions.rename(columns={"name": "region", "id": "region_id"}), on="region", how="left")
        .sort_values(["sales_manager", "region"])
        .reset_index(drop=True)
    )
    managers["id"] = managers.index + 1
    managers = managers.rename(columns={"sales_manager": "name"})[["id", "name", "region_id", "region"]]

    reps_base = (
        work_df[["sales_rep", "sales_manager", "region"]]
        .drop_duplicates()
        .sort_values(["sales_rep", "sales_manager", "region"])
        .reset_index(drop=True)
    )

    rep_conflicts = reps_base.groupby("sales_rep").size()
    multi_manager_reps = rep_conflicts[rep_conflicts > 1]
    if not multi_manager_reps.empty:
        LOGGER.warning(
            "Found reps mapped to multiple managers/regions. Using first mapping for: %s",
            ", ".join(multi_manager_reps.index.tolist()),
        )
    reps_base = reps_base.drop_duplicates(subset=["sales_rep"], keep="first")

    reps = (
        reps_base.merge(
            managers[["id", "name", "region"]].rename(columns={"id": "manager_id", "name": "sales_manager"}),
            on=["sales_manager", "region"],
            how="left",
        )
        .sort_values("sales_rep")
        .reset_index(drop=True)
    )
    reps["id"] = reps.index + 1
    reps = reps.rename(columns={"sales_rep": "name"})[["id", "name", "manager_id"]]

    fact = work_df.merge(
        reps.rename(columns={"id": "rep_id", "name": "sales_rep"}),
        on="sales_rep",
        how="left",
    )

    fact["visit_target"] = to_int_series(fact["visit_target"])
    fact["visits_made"] = to_int_series(fact["visits_made"])
    fact["call_target"] = to_int_series(fact["call_target"])
    fact["calls_made"] = to_int_series(fact["calls_made"])
    fact["quote_target"] = to_int_series(fact["quote_target"])
    fact["quotes_made"] = to_int_series(fact["quotes_made"])
    fact["call_attainment"] = ratio_to_int_percent(fact["call_attainment"])

    sales_perfomance = fact[
        [
            "rep_id",
            "month",
            "rev_target",
            "rev_actual",
            "rev_attainment",
            "rev_variance",
            "visit_target",
            "visits_made",
            "visit_attainment",
            "call_target",
            "calls_made",
            "call_attainment",
            "quote_target",
            "quotes_made",
            "quote_attainment",
            "pipeline_value",
            "pipeline_coverage",
            "ytd_actual",
            "status",
        ]
    ].rename(columns={"visits_made": "visit_made"})

    return {
        "regions": regions,
        "sales_managers": managers[["id", "name", "region_id"]],
        "sales_reps": reps,
        "sales_perfomance": sales_perfomance,
    }


def create_db_engine(config: PipelineConfig) -> Engine:
    return create_engine(config.connection_url)


def ensure_database_exists(config: PipelineConfig) -> None:
    db_name = config.pg_database
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", db_name):
        raise ValueError(f"Unsafe database name '{db_name}'. Use letters, numbers and underscores only.")

    bootstrap_url = config.connection_url.set(database=config.pg_bootstrap_db)
    bootstrap_engine = create_engine(bootstrap_url, isolation_level="AUTOCOMMIT")

    with bootstrap_engine.connect() as conn:
        exists = conn.execute(
            text("SELECT 1 FROM pg_database WHERE datname = :db_name"),
            {"db_name": db_name},
        ).scalar()
        if not exists:
            LOGGER.info("Database '%s' does not exist. Creating it...", db_name)
            conn.exec_driver_sql(f'CREATE DATABASE "{db_name}"')

    bootstrap_engine.dispose()


def ensure_target_tables(engine: Engine) -> None:
    ddl = """
    CREATE TABLE IF NOT EXISTS regions(
        id SERIAL PRIMARY KEY,
        name TEXT UNIQUE NOT NULL
    );

    CREATE TABLE IF NOT EXISTS sales_managers(
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        region_id INTEGER REFERENCES regions(id)
    );

    CREATE TABLE IF NOT EXISTS sales_reps(
        id SERIAL PRIMARY KEY,
        name TEXT UNIQUE NOT NULL,
        manager_id INTEGER REFERENCES sales_managers(id)
    );

    CREATE TABLE IF NOT EXISTS sales_perfomance(
        id SERIAL PRIMARY KEY,
        rep_id INTEGER REFERENCES sales_reps(id),
        month TEXT,
        rev_target NUMERIC,
        rev_actual NUMERIC,
        rev_attainment NUMERIC,
        rev_variance NUMERIC,
        visit_target INTEGER,
        visit_made INTEGER,
        visit_attainment NUMERIC,
        call_target INTEGER,
        calls_made INTEGER,
        call_attainment INTEGER,
        quote_target INTEGER,
        quotes_made INTEGER,
        quote_attainment NUMERIC,
        pipeline_value NUMERIC,
        pipeline_coverage NUMERIC,
        ytd_actual NUMERIC,
        status TEXT
    );
    """

    with engine.begin() as conn:
        conn.exec_driver_sql(ddl)


def create_powerbi_view(engine: Engine) -> None:
    view_sql = """
    CREATE OR REPLACE VIEW vw_powerbi_sales_performance AS
    SELECT
        sp.id AS performance_id,
        r.name AS region,
        sm.name AS sales_manager,
        sr.name AS sales_rep,
        sp.month,
        sp.rev_target,
        sp.rev_actual,
        sp.rev_attainment,
        sp.rev_variance,
        sp.visit_target,
        sp.visit_made,
        sp.visit_attainment,
        sp.call_target,
        sp.calls_made,
        sp.call_attainment,
        sp.quote_target,
        sp.quotes_made,
        sp.quote_attainment,
        sp.pipeline_value,
        sp.pipeline_coverage,
        sp.ytd_actual,
        sp.status
    FROM sales_perfomance sp
    JOIN sales_reps sr ON sp.rep_id = sr.id
    JOIN sales_managers sm ON sr.manager_id = sm.id
    JOIN regions r ON sm.region_id = r.id;
    """

    with engine.begin() as conn:
        conn.exec_driver_sql(view_sql)


def load_to_postgres(model_tables: dict[str, pd.DataFrame], engine: Engine) -> None:
    ensure_target_tables(engine)

    with engine.begin() as conn:
        conn.exec_driver_sql(
            "TRUNCATE TABLE sales_perfomance, sales_reps, sales_managers, regions RESTART IDENTITY CASCADE;"
        )

    for table_name in ["regions", "sales_managers", "sales_reps", "sales_perfomance"]:
        table_df = model_tables[table_name]
        LOGGER.info("Loading %s (%s rows)...", table_name, len(table_df))
        table_df.to_sql(
            name=table_name,
            con=engine,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000,
        )

    create_powerbi_view(engine)


def run_pipeline() -> None:
    config = load_config()
    LOGGER.info("Starting ETL pipeline")
    LOGGER.info("Source file: %s", config.input_file)

    if not config.input_file.exists():
        raise FileNotFoundError(f"Input file not found: {config.input_file}")

    raw_df = read_excel_with_retry(config.input_file)
    cleaned_df = clean_data(raw_df)
    model_tables = build_relational_model(cleaned_df)

    try:
        ensure_database_exists(config)
    except OperationalError as exc:
        raise RuntimeError(
            "Could not connect to PostgreSQL bootstrap database. Check PG_HOST/PG_PORT/PG_USER/PG_PASSWORD and permissions."
        ) from exc

    engine = create_db_engine(config)

    try:
        load_to_postgres(model_tables, engine)
    except OperationalError as exc:
        raise RuntimeError(
            "Could not connect to PostgreSQL. Check PG_HOST/PG_PORT/PG_DATABASE/PG_USER/PG_PASSWORD and ensure PostgreSQL accepts TCP connections."
        ) from exc

    LOGGER.info("ETL pipeline completed successfully")
    LOGGER.info("Power BI objects ready: regions, sales_managers, sales_reps, sales_perfomance, vw_powerbi_sales_performance")


if __name__ == "__main__":
    run_pipeline()
