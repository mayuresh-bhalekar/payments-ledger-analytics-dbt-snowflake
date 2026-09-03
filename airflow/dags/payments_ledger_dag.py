from __future__ import annotations

import os
import subprocess
from datetime import datetime

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import Variable, dag, task

DBT_VENV_BIN = "/opt/dbt_venv/bin/dbt"
SNOWFLAKE_CONN_ID = "snowflake_payments_ledger"

DEFAULT_DBT_PROJECT_DIR = "/opt/airflow/payments-ledger-analytics-dbt-snowflake/dbt_project"


def _dbt_project_dir() -> str:
    return Variable.get("dbt_project_dir", default=DEFAULT_DBT_PROJECT_DIR)


def _snowflake_env() -> dict:
    conn = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID).get_connection(
        SNOWFLAKE_CONN_ID
    )
    extra = conn.extra_dejson or {}

    env = dict(os.environ)
    env.update(
        {
            "SNOWFLAKE_ACCOUNT": extra.get("account", ""),
            "SNOWFLAKE_AIRFLOW_USER": conn.login or "",
            "SNOWFLAKE_AIRFLOW_PASSWORD": conn.password or "",
            "SNOWFLAKE_ROLE": extra.get("role", "DBT_TRANSFORMER"),
            "SNOWFLAKE_DATABASE": extra.get("database", "PAYMENTS_LEDGER"),
            "SNOWFLAKE_WAREHOUSE": extra.get("warehouse", "WH_DBT_TRANSFORM"),
            "SNOWFLAKE_AIRFLOW_SCHEMA": extra.get("schema", "orchestrated"),
            "DBT_PROFILES_DIR": _dbt_project_dir(),
        }
    )
    return env


def _run_dbt(*args: str) -> None:
    cmd = [DBT_VENV_BIN, *args, "--target", "airflow"]
    project_dir = _dbt_project_dir()
    result = subprocess.run(
        cmd,
        cwd=project_dir,
        env=_snowflake_env(),
        capture_output=True,
        text=True,
        check=False,
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError(
            f"dbt {' '.join(args)} failed (exit {result.returncode}) in {project_dir}"
        )


@dag(
    dag_id="payments_ledger_analytics",
    description="Settlement ingestion -> Snowflake RAW -> dbt staging -> "
    "snapshot -> ledger + semantic layer -> tests",
    schedule="0 5 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["payments", "dbt", "snowflake", "finance"],
    default_args={"retries": 1},
)
def payments_ledger_analytics():
    @task
    def dbt_deps() -> None:
        _run_dbt("deps")

    @task
    def dbt_seed() -> None:
        _run_dbt("seed")

    @task
    def dbt_run_staging() -> None:
        _run_dbt("run", "--select", "staging")

    @task
    def dbt_snapshot() -> None:
        _run_dbt("snapshot")

    @task
    def dbt_run_marts() -> None:
        _run_dbt("run", "--exclude", "staging")

    @task
    def dbt_test() -> None:
        _run_dbt("test")

    (
        dbt_deps()
        >> dbt_seed()
        >> dbt_run_staging()
        >> dbt_snapshot()
        >> dbt_run_marts()
        >> dbt_test()
    )


payments_ledger_analytics()
