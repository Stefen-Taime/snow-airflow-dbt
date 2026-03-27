"""
DAG 2: tlc_dbt_transform
=========================
Triggered by DAG 1 (tlc_raw_ingestion) after raw data is loaded.

Steps:
  1. dbt source freshness  — verify raw data is fresh
  2. dbt seed              — load reference CSVs (rate_codes, payment_types, vendor_lookup)
  3. dbt build             — run + test all models (staging → intermediate → marts)
  4. elementary report     — generate data quality HTML report

Uses BashOperator for dbt CLI commands.
Cosmos DbtTaskGroup can be added later for model-level observability.

Ref: spec.md Section 4.3
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

import sys

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

sys.path.insert(0, os.path.join(os.environ.get("AIRFLOW_HOME", "/usr/local/airflow"), "include"))
from slack_alerts import on_dag_success, on_dag_failure, on_task_failure

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DBT_PROJECT_DIR = os.environ.get(
    "DBT_PROJECT_DIR",
    os.path.join(os.environ.get("AIRFLOW_HOME", "/usr/local/airflow"), "include", "dbt_project"),
)
DBT_PROFILES_DIR = os.environ.get("DBT_PROFILES_DIR", DBT_PROJECT_DIR)
SNOWFLAKE_CONN_ID = "snowflake_default"

# Common dbt CLI prefix
DBT_CMD = f"dbt --no-use-colors"
DBT_GLOBAL_FLAGS = f"--project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}"

DEFAULT_ARGS = {
    "owner": "stefen",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "on_failure_callback": on_task_failure,
}


# ---------------------------------------------------------------------------
# DAG Definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="tlc_dbt_transform",
    description="DAG 2: dbt source freshness → seed → build → elementary report",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule=None,  # Triggered by DAG 1
    catchup=False,
    tags=["tlc", "dbt", "transform", "snowflake"],
    doc_md=__doc__,
    on_success_callback=on_dag_success,
    on_failure_callback=on_dag_failure,
) as dag:

    # === 1. dbt source freshness ============================================
    task_source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=f"{DBT_CMD} source freshness {DBT_GLOBAL_FLAGS}",
    )

    # === 2. dbt seed ========================================================
    task_dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"{DBT_CMD} seed {DBT_GLOBAL_FLAGS}",
    )

    # === 3. dbt build =======================================================
    # Uses --select source_status:fresher+ to only build models
    # whose sources have been refreshed (saves Snowflake credits).
    task_dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"{DBT_CMD} build {DBT_GLOBAL_FLAGS}",
    )

    # === 4. elementary report ===============================================
    task_elementary_report = BashOperator(
        task_id="elementary_report",
        bash_command=(
            f"edr report --project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR} "
            f"--output-path {DBT_PROJECT_DIR}/target/elementary_report.html"
        ),
    )

    # === Dependencies ========================================================
    task_source_freshness >> task_dbt_seed >> task_dbt_build >> task_elementary_report
