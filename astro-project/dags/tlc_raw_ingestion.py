"""
DAG 1: tlc_raw_ingestion
========================
Orchestrated by Astronomer Cloud (CI/CD via GitHub Actions).
Downloads NYC TLC trip data (Parquet + CSV) from the TLC CDN,
stages files into Snowflake internal stages, and loads them
into RAW tables via COPY INTO.

Triggers DAG 2 (tlc_dbt_transform) on success.

Ref: spec.md Section 4.2
"""

from __future__ import annotations

import os
import json
import logging
from datetime import datetime, timedelta

import requests
from airflow.sdk import DAG, task_group
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable

import sys
sys.path.insert(0, os.path.join(os.environ.get("AIRFLOW_HOME", "/usr/local/airflow"), "include"))
from slack_alerts import on_dag_success, on_dag_failure, on_task_failure

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SNOWFLAKE_CONN_ID = "snowflake_default"
TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net"
TMP_DIR = "/tmp/tlc_data"
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/usr/local/airflow")
INCLUDE_SQL_DIR = os.path.join(AIRFLOW_HOME, "include", "sql")

DEFAULT_ARGS = {
    "owner": "stefen",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_task_failure,
}


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------
def _get_data_months() -> list[str]:
    """Return list of year-month strings to process from Airflow variable."""
    raw = Variable.get("tlc_data_months", default_var='["2026-01"]')
    return json.loads(raw)


def _download_file(url: str, dest: str) -> str:
    """Download a file from URL to local dest. Returns dest path."""
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    logger.info("Downloading %s -> %s", url, dest)
    resp = requests.get(url, timeout=300, stream=True)
    resp.raise_for_status()
    with open(dest, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8 * 1024 * 1024):
            f.write(chunk)
    size_mb = os.path.getsize(dest) / (1024 * 1024)
    logger.info("Downloaded %.1f MB -> %s", size_mb, dest)
    return dest


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------
def download_trip_files(**context):
    """Download yellow + green Parquet files for all configured months."""
    months = _get_data_months()
    downloaded = []
    for month in months:
        for taxi_type in ("yellow", "green"):
            filename = f"{taxi_type}_tripdata_{month}.parquet"
            url = f"{TLC_BASE_URL}/trip-data/{filename}"
            dest = f"{TMP_DIR}/{taxi_type}/{filename}"
            try:
                _download_file(url, dest)
                downloaded.append(dest)
            except requests.exceptions.HTTPError as e:
                logger.warning("Failed to download %s: %s", url, e)
    logger.info("Downloaded %d trip files", len(downloaded))
    return downloaded


def download_zone_lookup(**context):
    """Download taxi zone lookup CSV."""
    url = f"{TLC_BASE_URL}/misc/taxi_zone_lookup.csv"
    dest = f"{TMP_DIR}/reference/taxi_zone_lookup.csv"
    _download_file(url, dest)
    return dest


# ---------------------------------------------------------------------------
# DAG Definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="tlc_raw_ingestion",
    description="DAG 1: Download TLC data, stage in Snowflake, COPY INTO raw tables",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule="@monthly",
    catchup=False,
    tags=["tlc", "ingestion", "snowflake"],
    doc_md=__doc__,
    template_searchpath=[INCLUDE_SQL_DIR],
    on_success_callback=on_dag_success,
    on_failure_callback=on_dag_failure,
) as dag:

    # === TaskGroup: Trip Data ===============================================
    @task_group(group_id="trip_data")
    def trip_data_group():
        download_trips = PythonOperator(
            task_id="download_trip_files",
            python_callable=download_trip_files,
        )

        # PUT yellow Parquet files into Snowflake internal stage
        stage_yellow = SQLExecuteQueryOperator(
            task_id="stage_yellow_files",
            conn_id=SNOWFLAKE_CONN_ID,
            sql=f"PUT 'file://{TMP_DIR}/yellow/*.parquet' @RAW.TLC_TRIPS.tlc_stage/yellow/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE",
            autocommit=True,
        )

        # PUT green Parquet files into Snowflake internal stage
        stage_green = SQLExecuteQueryOperator(
            task_id="stage_green_files",
            conn_id=SNOWFLAKE_CONN_ID,
            sql=f"PUT 'file://{TMP_DIR}/green/*.parquet' @RAW.TLC_TRIPS.tlc_stage/green/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE",
            autocommit=True,
        )

        # COPY INTO raw tables (filenames resolved via template_searchpath)
        copy_yellow = SQLExecuteQueryOperator(
            task_id="copy_into_yellow",
            conn_id=SNOWFLAKE_CONN_ID,
            sql="copy_into_yellow.sql",
            autocommit=True,
        )

        copy_green = SQLExecuteQueryOperator(
            task_id="copy_into_green",
            conn_id=SNOWFLAKE_CONN_ID,
            sql="copy_into_green.sql",
            autocommit=True,
        )

        download_trips >> [stage_yellow, stage_green]
        stage_yellow >> copy_yellow
        stage_green >> copy_green

    # === TaskGroup: Reference Data ==========================================
    @task_group(group_id="reference_data")
    def reference_data_group():
        download_lookup = PythonOperator(
            task_id="download_zone_lookup",
            python_callable=download_zone_lookup,
        )

        stage_lookup = SQLExecuteQueryOperator(
            task_id="stage_lookup_file",
            conn_id=SNOWFLAKE_CONN_ID,
            sql=f"PUT 'file://{TMP_DIR}/reference/taxi_zone_lookup.csv' @RAW.TLC_REFERENCE.tlc_ref_stage/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE",
            autocommit=True,
        )

        copy_lookup = SQLExecuteQueryOperator(
            task_id="copy_into_zone_lookup",
            conn_id=SNOWFLAKE_CONN_ID,
            sql="copy_into_zone_lookup.sql",
            autocommit=True,
        )

        download_lookup >> stage_lookup >> copy_lookup

    # === Cleanup staged files ===============================================
    cleanup = SQLExecuteQueryOperator(
        task_id="cleanup_staged_files",
        conn_id=SNOWFLAKE_CONN_ID,
        sql=[
            "REMOVE @RAW.TLC_TRIPS.tlc_stage;",
            "REMOVE @RAW.TLC_REFERENCE.tlc_ref_stage;",
        ],
        split_statements=True,
        autocommit=True,
    )

    # === Trigger DAG 2 ======================================================
    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt_dag",
        trigger_dag_id="tlc_dbt_transform",
        wait_for_completion=False,
    )

    # === Dependencies ========================================================
    trips = trip_data_group()
    refs = reference_data_group()

    [trips, refs] >> cleanup >> trigger_dbt
