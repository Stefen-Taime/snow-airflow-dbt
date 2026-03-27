# TLC NYC Taxi Data Pipeline — Project Specification

> **Usage**: This document is the single source of truth for building the project.
> Feed it to Claude Code, Cursor, or Codex as context before each implementation step.

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Architecture Overview](#2-architecture-overview)
3. [Step 1 — Snowflake Setup](#3-step-1--snowflake-setup)
4. [Step 2 — Airflow DAGs (Astronomer)](#4-step-2--airflow-dags-astronomer)
5. [Step 3 — dbt Project Structure](#5-step-3--dbt-project-structure)
6. [Step 4 — dbt Models (SQL)](#6-step-4--dbt-models-sql)
7. [Step 5 — dbt Seeds & Snapshots](#7-step-5--dbt-seeds--snapshots)
8. [Step 6 — Data Quality & Testing](#8-step-6--data-quality--testing)
9. [Step 7 — CI/CD Pipeline](#9-step-7--cicd-pipeline)
10. [Step 8 — Streamlit Dashboards](#10-step-8--streamlit-dashboards)
11. [Step 9 — Grafana FinOps Dashboard](#11-step-9--grafana-finops-dashboard)
12. [Appendix A — TLC Reference Data](#appendix-a--tlc-reference-data)
13. [Appendix B — KPI Catalog](#appendix-b--kpi-catalog)

---

## 1. Project Overview

| Field | Value |
|---|---|
| **Project Name** | `snow-airflow-dbt` |
| **Objective** | Production-grade ELT pipeline ingesting NYC TLC taxi trip data into Snowflake, transforming with dbt, visualizing with Streamlit+Plotly, and monitoring costs with Grafana |
| **Cloud** | Snowflake (30-day trial, 400$ credits) |
| **Orchestration** | Astronomer (Astro CLI) / Apache Airflow |
| **Transformation** | dbt-core with dbt-snowflake adapter |
| **Visualization** | Streamlit multi-page app + Plotly |
| **FinOps Monitoring** | Grafana Cloud (14-day trial) + Snowflake Plugin |
| **CI/CD** | GitHub Actions + SQLFluff + dbt_project_evaluator |
| **Data Source** | [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) |

### Data Sources

| Source | Format | Volume | Frequency |
|---|---|---|---|
| Yellow Taxi Trips | Parquet | ~3M rows/month | Monthly batch (~2-month publication delay) |
| Green Taxi Trips | Parquet | ~80K rows/month | Monthly batch (~2-month publication delay) |
| Taxi Zone Lookup | CSV | 265 rows | Static (rarely changes) |
| Data Dictionaries | PDF | N/A | Reference only (rate codes, payment types, vendor IDs) |

### Key Architectural Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Raw layer format | `MATCH_BY_COLUMN_NAME` (Option B) | TLC schema is stable and documented; cheaper and faster than VARIANT |
| Schema evolution | `ENABLE_SCHEMA_EVOLUTION = TRUE` | Auto-add new columns (e.g. `cbd_congestion_fee` added 2025) |
| Staging | Snowflake Internal Stage | Simpler than external S3; no AWS setup needed |
| Idempotence | `COPY INTO` with `ON_ERROR = ABORT_STATEMENT` | Reproducible runs, no duplication |
| Orchestration | 2 DAGs (ingestion + transform) | Separation of concerns; transform triggers only after successful ingestion |
| Reference data mapping | dbt seeds for rate_codes/payment_types; Airflow DAG for taxi_zone_lookup | Seeds = git-versioned; lookup = sourced from TLC CDN |
| Incremental strategy | `delete+insert` for staging; `microbatch` for marts | Handles late-arriving data with lookback |
| Anomaly detection | Elementary (inline, same DAG) | Small data volume; no need for separate observability DAG |
| SQL linting | SQLFluff (dialect: snowflake, templater: dbt) | Mature, standard de facto for dbt projects |
| Dashboard framework | Streamlit multi-page + Plotly | Free deployment on Streamlit Community Cloud |

---

## 2. Architecture Overview

```
┌─────────────────┐     ┌───────────────────────────┐     ┌──────────────────────────┐
│  TLC CDN        │     │  Astronomer (Astro CLI)    │     │  Snowflake               │
│  (Parquet/CSV)  │────▶│  Apache Airflow            │────▶│                          │
│                 │     │                           │     │  RAW database            │
│  • yellow trips │     │  DAG 1: tlc_raw_ingestion  │     │  ├─ TLC_TRIPS schema     │
│  • green trips  │     │  ├─ download files          │     │  │  ├─ yellow_taxi_trips  │
│  • zone lookup  │     │  ├─ PUT → internal stage    │     │  │  └─ green_taxi_trips   │
│                 │     │  ├─ COPY INTO raw tables    │     │  └─ TLC_REFERENCE schema  │
│                 │     │  └─ trigger DAG 2           │     │     └─ taxi_zone_lookup   │
└─────────────────┘     │                           │     │                          │
                        │  DAG 2: tlc_dbt_transform  │     │  ANALYTICS database      │
                        │  ├─ dbt source freshness    │     │  ├─ staging schema       │
                        │  ├─ dbt seed                │     │  ├─ intermediate schema  │
                        │  ├─ dbt build               │     │  ├─ snapshots schema     │
                        │  └─ elementary report       │     │  └─ marts schemas        │
                        └───────────────────────────┘     │     ├─ core              │
                                                          │     ├─ executive          │
                                                          │     ├─ revenue            │
                                                          │     ├─ geographic         │
                                                          │     ├─ congestion         │
                                                          │     └─ operations         │
                                                          └──────────────────────────┘
                                                                       │
                                                                       ▼
                                                          ┌──────────────────────────┐
                                                          │  Streamlit + Plotly      │
                                                          │  (Community Cloud)       │
                                                          │                          │
                                                          │  5 dashboard pages       │
                                                          │  ├─ Executive Overview   │
                                                          │  ├─ Revenue Analysis     │
                                                          │  ├─ Geographic Intel     │
                                                          │  ├─ Congestion Pricing   │
                                                          │  └─ Operations           │
                                                          └──────────────────────────┘

                        ┌───────────────────────────┐
                        │  Grafana Cloud (free)     │
                        │  Snowflake Plugin         │
                        │                           │
                        │  FinOps Dashboard         │
                        │  ├─ Credit Burn Rate      │
                        │  ├─ Warehouse Metrics     │
                        │  ├─ Top Queries           │
                        │  ├─ Storage Usage         │
                        │  └─ Budget Alerts         │
                        └───────────────────────────┘
```

---

## 3. Step 1 — Snowflake Setup

### 3.1 Database & Schema Creation

```sql
-- Databases
CREATE DATABASE IF NOT EXISTS RAW;
CREATE DATABASE IF NOT EXISTS ANALYTICS;

-- RAW schemas
CREATE SCHEMA IF NOT EXISTS RAW.TLC_TRIPS;
CREATE SCHEMA IF NOT EXISTS RAW.TLC_REFERENCE;

-- ANALYTICS schemas (dbt will create these, but declare for clarity)
-- staging, intermediate, snapshots, core, executive, revenue, geographic, congestion, operations
```

### 3.2 Raw Tables (MATCH_BY_COLUMN_NAME)

```sql
-- Yellow taxi trips
CREATE OR REPLACE TABLE RAW.TLC_TRIPS.yellow_taxi_trips (
    VendorID              INTEGER,
    tpep_pickup_datetime  TIMESTAMP_NTZ,
    tpep_dropoff_datetime TIMESTAMP_NTZ,
    passenger_count       FLOAT,
    trip_distance         FLOAT,
    RatecodeID            FLOAT,
    store_and_fwd_flag    VARCHAR,
    PULocationID          INTEGER,
    DOLocationID          INTEGER,
    payment_type          INTEGER,
    fare_amount           FLOAT,
    extra                 FLOAT,
    mta_tax               FLOAT,
    tip_amount            FLOAT,
    tolls_amount          FLOAT,
    improvement_surcharge FLOAT,
    total_amount          FLOAT,
    congestion_surcharge  FLOAT,
    Airport_fee           FLOAT,
    cbd_congestion_fee    FLOAT,
    load_timestamp        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Green taxi trips
CREATE OR REPLACE TABLE RAW.TLC_TRIPS.green_taxi_trips (
    VendorID              INTEGER,
    lpep_pickup_datetime  TIMESTAMP_NTZ,
    lpep_dropoff_datetime TIMESTAMP_NTZ,
    passenger_count       FLOAT,
    trip_distance         FLOAT,
    RatecodeID            FLOAT,
    store_and_fwd_flag    VARCHAR,
    PULocationID          INTEGER,
    DOLocationID          INTEGER,
    payment_type          INTEGER,
    fare_amount           FLOAT,
    extra                 FLOAT,
    mta_tax               FLOAT,
    tip_amount            FLOAT,
    tolls_amount          FLOAT,
    improvement_surcharge FLOAT,
    total_amount          FLOAT,
    congestion_surcharge  FLOAT,
    Airport_fee           FLOAT,
    cbd_congestion_fee    FLOAT,
    load_timestamp        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Taxi zone lookup
CREATE OR REPLACE TABLE RAW.TLC_REFERENCE.taxi_zone_lookup (
    LocationID   INTEGER,
    Borough      VARCHAR,
    Zone         VARCHAR,
    service_zone VARCHAR,
    load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

### 3.3 Stages & File Formats

```sql
CREATE OR REPLACE STAGE RAW.TLC_TRIPS.tlc_stage;
CREATE OR REPLACE STAGE RAW.TLC_REFERENCE.tlc_ref_stage;

CREATE OR REPLACE FILE FORMAT RAW.TLC_TRIPS.parquet_format
    TYPE = PARQUET;

CREATE OR REPLACE FILE FORMAT RAW.TLC_REFERENCE.csv_format
    TYPE = CSV
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1;
```

### 3.4 COPY INTO Commands

```sql
-- Yellow trips (MATCH_BY_COLUMN_NAME)
COPY INTO RAW.TLC_TRIPS.yellow_taxi_trips
FROM @RAW.TLC_TRIPS.tlc_stage/yellow/
FILE_FORMAT = (FORMAT_NAME = 'parquet_format')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = ABORT_STATEMENT;

-- Green trips
COPY INTO RAW.TLC_TRIPS.green_taxi_trips
FROM @RAW.TLC_TRIPS.tlc_stage/green/
FILE_FORMAT = (FORMAT_NAME = 'parquet_format')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = ABORT_STATEMENT;

-- Zone lookup
COPY INTO RAW.TLC_REFERENCE.taxi_zone_lookup (LocationID, Borough, Zone, service_zone)
FROM @RAW.TLC_REFERENCE.tlc_ref_stage/
FILE_FORMAT = (FORMAT_NAME = 'csv_format')
ON_ERROR = ABORT_STATEMENT;
```

### 3.5 Schema Evolution

```sql
ALTER TABLE RAW.TLC_TRIPS.yellow_taxi_trips
    SET ENABLE_SCHEMA_EVOLUTION = TRUE;

ALTER TABLE RAW.TLC_TRIPS.green_taxi_trips
    SET ENABLE_SCHEMA_EVOLUTION = TRUE;
```

### 3.6 FinOps — Resource Monitor

```sql
-- CRITICAL: Protect your 400$ credits
CREATE OR REPLACE RESOURCE MONITOR tlc_budget_monitor
    WITH CREDIT_QUOTA = 100  -- ~$350 worth at standard pricing
    FREQUENCY = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS
        ON 50 PERCENT DO NOTIFY
        ON 80 PERCENT DO NOTIFY
        ON 95 PERCENT DO SUSPEND
        ON 100 PERCENT DO SUSPEND_IMMEDIATE;

ALTER WAREHOUSE COMPUTE_WH SET RESOURCE_MONITOR = tlc_budget_monitor;
```

### 3.7 Warehouse Configuration

```sql
-- XS warehouse for TLC data (small volume)
CREATE OR REPLACE WAREHOUSE TLC_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60          -- 60 seconds (NOT the 10-min default)
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'TLC pipeline warehouse - auto-suspend 60s to preserve credits';
```

---

## 4. Step 2 — Airflow DAGs (Astronomer)

### 4.1 Project Structure

```
astro-project/
├── dags/
│   ├── tlc_raw_ingestion.py        # DAG 1: Download + Stage + COPY INTO
│   └── tlc_dbt_transform.py        # DAG 2: dbt source freshness + seed + build + elementary
├── include/
│   └── sql/
│       ├── copy_into_yellow.sql
│       ├── copy_into_green.sql
│       └── copy_into_zone_lookup.sql
├── requirements.txt                 # apache-airflow-providers-snowflake
├── Dockerfile
└── airflow_settings.yaml            # Snowflake connection (key pair auth recommended)
```

### 4.2 DAG 1: `tlc_raw_ingestion`

```
Schedule: @monthly (or manual trigger)
Connection: Snowflake key pair auth

TaskGroup: trip_data
├── download_yellow_jan         (HttpOperator or PythonOperator → requests.get)
├── download_yellow_feb
├── download_green_jan
├── download_green_feb
├── stage_trip_files            (SnowflakeOperator → PUT)
└── copy_into_raw_trips         (SnowflakeOperator → COPY INTO)

TaskGroup: reference_data
├── download_taxi_zone_lookup   (PythonOperator → requests.get)
├── stage_lookup_file           (SnowflakeOperator → PUT)
└── copy_into_ref_lookup        (SnowflakeOperator → COPY INTO)

cleanup_staged_files            (SnowflakeOperator → REMOVE @stage)

trigger_dbt_dag                 (TriggerDagRunOperator → DAG 2)
```

**TLC CDN URL pattern:**
```
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2026-01.parquet
https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2026-01.parquet
https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

### 4.3 DAG 2: `tlc_dbt_transform`

```
Schedule: None (triggered by DAG 1)

task_source_freshness           (BashOperator → dbt source freshness)
  └── PASS → continue
  └── FAIL → alert + stop

task_dbt_seed                   (BashOperator → dbt seed)

task_dbt_build                  (BashOperator → dbt build --select source_status:fresher+)
  ├── Includes: run + test (generic, dbt-expectations, singular, elementary)
  └── Uses source_status:fresher+ to save credits

task_elementary_report          (BashOperator → edr report)
  └── Generates HTML report + optional Slack alert
```

### 4.4 Airflow Providers

```
# requirements.txt
apache-airflow-providers-snowflake>=5.0.0
astronomer-cosmos>=1.5.0          # dbt integration for Airflow (optional)
```

---

## 5. Step 3 — dbt Project Structure

### 5.1 Directory Layout

```
dbt_project/
├── dbt_project.yml
├── packages.yml
├── profiles.yml                    # Snowflake connection
├── .sqlfluff                       # SQL linting config
├── .pre-commit-config.yaml         # dbt-checkpoint hooks
│
├── seeds/                          # Static reference data (CSV, git-versioned)
│   ├── rate_codes.csv
│   ├── payment_types.csv
│   └── vendor_lookup.csv
│
├── snapshots/                      # SCD Type 2
│   └── snap_taxi_zone_lookup.sql
│
├── models/
│   ├── staging/                    # 1:1 cleaning of raw sources
│   │   ├── _sources.yml            # Source declarations + freshness
│   │   ├── _staging__models.yml    # Model configs + tests
│   │   ├── stg_yellow_taxi_trips.sql
│   │   ├── stg_green_taxi_trips.sql
│   │   └── stg_taxi_zone_lookup.sql
│   │
│   ├── intermediate/               # Shared business logic
│   │   ├── _intermediate__models.yml
│   │   ├── int_trips_unioned.sql   # Union yellow + green
│   │   └── int_trips_enriched.sql  # Join zones, rate codes, payment types
│   │
│   └── marts/                      # Exposed tables, organized by domain
│       ├── core/                   # Shared facts & dimensions
│       │   ├── _core__models.yml
│       │   ├── fct_taxi_trips.sql
│       │   ├── dim_zones.sql
│       │   ├── dim_vendors.sql
│       │   └── dim_rate_codes.sql
│       │
│       ├── executive/              # Dashboard 1: Executive Overview
│       │   ├── _executive__models.yml
│       │   └── met_executive_summary.sql
│       │
│       ├── revenue/                # Dashboard 2: Revenue & Fare Analysis
│       │   ├── _revenue__models.yml
│       │   ├── met_daily_revenue.sql
│       │   ├── met_revenue_by_zone.sql
│       │   ├── met_revenue_by_rate_code.sql
│       │   └── met_payment_distribution.sql
│       │
│       ├── geographic/             # Dashboard 3: Geographic & Zone Intel
│       │   ├── _geographic__models.yml
│       │   ├── met_zone_pickup_ranking.sql
│       │   ├── met_zone_pair_analysis.sql
│       │   ├── met_borough_summary.sql
│       │   └── met_airport_trips.sql
│       │
│       ├── congestion/             # Dashboard 4: CBD Congestion Pricing Impact
│       │   ├── _congestion__models.yml
│       │   ├── met_cbd_daily_impact.sql
│       │   ├── met_cbd_vs_non_cbd.sql
│       │   ├── met_cbd_peak_offpeak.sql
│       │   └── met_cbd_yellow_vs_green.sql
│       │
│       ├── operations/             # Dashboard 5: Operational Performance
│       │   ├── _operations__models.yml
│       │   ├── met_hourly_demand.sql
│       │   ├── met_trip_duration_dist.sql
│       │   ├── met_avg_speed_trend.sql
│       │   └── met_vendor_performance.sql
│       │
│       └── _exposures.yml          # Lineage: mart → Streamlit dashboard
│
├── tests/
│   ├── generic/
│   │   └── test_positive_value.sql
│   └── singular/
│       ├── assert_no_future_trips.sql
│       └── assert_fare_consistent_with_distance.sql
│
└── macros/                         # Reusable Jinja macros
    └── generate_surrogate_key.sql  # (or use dbt_utils)
```

### 5.2 `dbt_project.yml`

```yaml
name: 'tlc_project'
version: '1.0.0'
config-version: 2
profile: 'snowflake'

model-paths: ["models"]
seed-paths: ["seeds"]
test-paths: ["tests"]
snapshot-paths: ["snapshots"]
macro-paths: ["macros"]

clean-targets:
  - "target"
  - "dbt_packages"

seeds:
  tlc_project:
    +schema: reference
    rate_codes:
      +column_types:
        rate_code_id: integer
    payment_types:
      +column_types:
        payment_type_id: integer
    vendor_lookup:
      +column_types:
        vendor_id: integer

models:
  tlc_project:
    staging:
      +schema: staging
      +materialized: incremental
    intermediate:
      +schema: intermediate
      +materialized: ephemeral
    marts:
      core:
        +schema: core
        +materialized: incremental
      executive:
        +schema: executive
        +materialized: table
      revenue:
        +schema: revenue
        +materialized: table
      geographic:
        +schema: geographic
        +materialized: table
      congestion:
        +schema: congestion
        +materialized: table
      operations:
        +schema: operations
        +materialized: table

snapshots:
  tlc_project:
    +target_schema: snapshots
```

### 5.3 `packages.yml`

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: [">=1.0.0", "<2.0.0"]
  - package: calogica/dbt_expectations
    version: [">=0.10.0", "<1.0.0"]
  - package: dbt-labs/dbt_project_evaluator
    version: [">=0.8.0", "<1.0.0"]
  - package: elementary-data/elementary
    version: [">=0.15.0", "<1.0.0"]
```

---

## 6. Step 4 — dbt Models (SQL)

### 6.1 Sources — `models/staging/_sources.yml`

```yaml
version: 2

sources:
  - name: tlc_trips
    database: RAW
    schema: TLC_TRIPS
    freshness:
      warn_after: { count: 35, period: day }
      error_after: { count: 65, period: day }
    loaded_at_field: load_timestamp
    tables:
      - name: yellow_taxi_trips
        description: "Yellow taxi trip records from NYC TLC"
        columns:
          - name: VendorID
            tests:
              - not_null
      - name: green_taxi_trips
        description: "Green taxi trip records from NYC TLC"

  - name: tlc_reference
    database: RAW
    schema: TLC_REFERENCE
    freshness:
      warn_after: { count: 365, period: day }
      error_after: { count: 730, period: day }
    loaded_at_field: load_timestamp
    tables:
      - name: taxi_zone_lookup
        description: "265 NYC taxi zones with borough and service zone"
```

### 6.2 Staging — `stg_yellow_taxi_trips.sql`

```sql
{{
  config(
    materialized='incremental',
    unique_key='trip_surrogate_key',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
  )
}}

WITH source AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'VendorID', 'tpep_pickup_datetime',
            'tpep_dropoff_datetime', 'PULocationID', 'DOLocationID'
        ]) }} AS trip_surrogate_key,
        VendorID              AS vendor_id,
        tpep_pickup_datetime  AS pickup_datetime,
        tpep_dropoff_datetime AS dropoff_datetime,
        passenger_count,
        trip_distance,
        PULocationID          AS pu_location_id,
        DOLocationID          AS do_location_id,
        RatecodeID            AS rate_code_id,
        payment_type          AS payment_type_id,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge,
        Airport_fee           AS airport_fee,
        cbd_congestion_fee,
        'yellow'              AS taxi_type,
        load_timestamp
    FROM {{ source('tlc_trips', 'yellow_taxi_trips') }}
)

SELECT * FROM source

{% if is_incremental() %}
  WHERE pickup_datetime >= (
    SELECT DATEADD('day', -3, MAX(pickup_datetime))
    FROM {{ this }}
  )
{% endif %}
```

### 6.3 Staging — `stg_green_taxi_trips.sql`

```sql
{{
  config(
    materialized='incremental',
    unique_key='trip_surrogate_key',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
  )
}}

WITH source AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'VendorID', 'lpep_pickup_datetime',
            'lpep_dropoff_datetime', 'PULocationID', 'DOLocationID'
        ]) }} AS trip_surrogate_key,
        VendorID              AS vendor_id,
        lpep_pickup_datetime  AS pickup_datetime,
        lpep_dropoff_datetime AS dropoff_datetime,
        passenger_count,
        trip_distance,
        PULocationID          AS pu_location_id,
        DOLocationID          AS do_location_id,
        RatecodeID            AS rate_code_id,
        payment_type          AS payment_type_id,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge,
        Airport_fee           AS airport_fee,
        cbd_congestion_fee,
        'green'               AS taxi_type,
        load_timestamp
    FROM {{ source('tlc_trips', 'green_taxi_trips') }}
)

SELECT * FROM source

{% if is_incremental() %}
  WHERE pickup_datetime >= (
    SELECT DATEADD('day', -3, MAX(pickup_datetime))
    FROM {{ this }}
  )
{% endif %}
```

### 6.4 Staging — `stg_taxi_zone_lookup.sql`

```sql
{{
  config(
    materialized='table'
  )
}}

SELECT
    LocationID   AS location_id,
    Borough      AS borough,
    Zone         AS zone_name,
    service_zone
FROM {{ source('tlc_reference', 'taxi_zone_lookup') }}
WHERE LocationID NOT IN (264, 265)  -- Filter Unknown zones
```

### 6.5 Intermediate — `int_trips_unioned.sql`

```sql
{{
  config(
    materialized='ephemeral'
  )
}}

SELECT * FROM {{ ref('stg_yellow_taxi_trips') }}
UNION ALL
SELECT * FROM {{ ref('stg_green_taxi_trips') }}
```

### 6.6 Marts Core — `fct_taxi_trips.sql`

```sql
{{
  config(
    materialized='incremental',
    incremental_strategy='microbatch',
    event_time='pickup_datetime',
    begin='2026-01-01',
    batch_size='day',
    lookback=3,
    unique_key='trip_surrogate_key'
  )
}}

SELECT
    t.trip_surrogate_key,
    t.vendor_id,
    v.vendor_name,
    t.pickup_datetime,
    t.dropoff_datetime,
    t.passenger_count,
    t.trip_distance,
    t.pu_location_id,
    pu_zone.zone_name     AS pickup_zone,
    pu_zone.borough       AS pickup_borough,
    t.do_location_id,
    do_zone.zone_name     AS dropoff_zone,
    do_zone.borough       AS dropoff_borough,
    t.rate_code_id,
    rc.rate_code_name,
    t.payment_type_id,
    pt.payment_type_name,
    t.fare_amount,
    t.extra,
    t.mta_tax,
    t.tip_amount,
    t.tolls_amount,
    t.improvement_surcharge,
    t.total_amount,
    t.congestion_surcharge,
    t.airport_fee,
    t.cbd_congestion_fee,
    t.taxi_type
FROM {{ ref('int_trips_unioned') }} t
LEFT JOIN {{ ref('dim_zones') }} pu_zone
    ON t.pu_location_id = pu_zone.location_id
LEFT JOIN {{ ref('dim_zones') }} do_zone
    ON t.do_location_id = do_zone.location_id
LEFT JOIN {{ ref('rate_codes') }} rc
    ON t.rate_code_id = rc.rate_code_id
LEFT JOIN {{ ref('payment_types') }} pt
    ON t.payment_type_id = pt.payment_type_id
LEFT JOIN {{ ref('vendor_lookup') }} v
    ON t.vendor_id = v.vendor_id
```

### 6.7 Marts Core — `dim_zones.sql`

```sql
{{
  config(
    materialized='table'
  )
}}

SELECT
    location_id,
    borough,
    zone_name,
    service_zone
FROM {{ ref('stg_taxi_zone_lookup') }}
```

### 6.8 Marts Executive — `met_executive_summary.sql`

```sql
{{
  config(
    materialized='table'
  )
}}

SELECT
    DATE_TRUNC('month', pickup_datetime)              AS trip_month,
    taxi_type,
    COUNT(*)                                           AS total_trips,
    SUM(total_amount)                                  AS total_revenue,
    AVG(fare_amount)                                   AS avg_fare,
    AVG(trip_distance)                                 AS avg_trip_distance,
    AVG(passenger_count)                               AS avg_passenger_count,
    SUM(cbd_congestion_fee)                            AS total_cbd_fees,
    COUNT(CASE WHEN cbd_congestion_fee > 0 THEN 1 END) AS cbd_trips
FROM {{ ref('fct_taxi_trips') }}
GROUP BY 1, 2
```

### 6.9 Marts Congestion — `met_cbd_daily_impact.sql`

```sql
{{
  config(
    materialized='table'
  )
}}

SELECT
    DATE_TRUNC('day', pickup_datetime)                  AS trip_date,
    taxi_type,
    COUNT(*)                                             AS total_trips,
    COUNT(CASE WHEN cbd_congestion_fee > 0 THEN 1 END)  AS cbd_trips,
    ROUND(
      COUNT(CASE WHEN cbd_congestion_fee > 0 THEN 1 END) * 100.0
      / NULLIF(COUNT(*), 0), 2
    )                                                    AS cbd_trip_pct,
    SUM(cbd_congestion_fee)                              AS total_cbd_fees,
    AVG(CASE WHEN cbd_congestion_fee > 0
        THEN cbd_congestion_fee END)                     AS avg_cbd_fee,
    SUM(total_amount)                                    AS total_revenue,
    AVG(total_amount)                                    AS avg_revenue_per_trip,
    SUM(total_amount) / NULLIF(SUM(trip_distance), 0)    AS revenue_per_mile,
    AVG(trip_distance / NULLIF(
        DATEDIFF('minute', pickup_datetime, dropoff_datetime) / 60.0, 0
    ))                                                   AS avg_speed_mph
FROM {{ ref('fct_taxi_trips') }}
GROUP BY 1, 2
```

### 6.10 Marts Congestion — `met_cbd_vs_non_cbd.sql`

```sql
{{
  config(
    materialized='table'
  )
}}

SELECT
    DATE_TRUNC('month', pickup_datetime)                AS trip_month,
    taxi_type,
    CASE WHEN cbd_congestion_fee > 0 THEN 'CBD Zone' ELSE 'Non-CBD Zone' END AS zone_category,
    COUNT(*)                                             AS total_trips,
    AVG(total_amount)                                    AS avg_revenue_per_trip,
    SUM(total_amount) / NULLIF(SUM(trip_distance), 0)    AS revenue_per_mile,
    AVG(tip_amount) / NULLIF(AVG(fare_amount), 0) * 100  AS avg_tip_pct,
    AVG(trip_distance / NULLIF(
        DATEDIFF('minute', pickup_datetime, dropoff_datetime) / 60.0, 0
    ))                                                   AS avg_speed_mph,
    AVG(DATEDIFF('minute', pickup_datetime, dropoff_datetime)) AS avg_duration_min
FROM {{ ref('fct_taxi_trips') }}
GROUP BY 1, 2, 3
```

### 6.11 Marts Congestion — `met_cbd_peak_offpeak.sql`

```sql
{{
  config(
    materialized='table'
  )
}}

SELECT
    DATE_TRUNC('month', pickup_datetime)              AS trip_month,
    taxi_type,
    CASE
        WHEN DAYOFWEEK(pickup_datetime) IN (0, 6) -- weekend
             AND HOUR(pickup_datetime) BETWEEN 9 AND 20
             THEN 'Peak (Weekend 9am-9pm)'
        WHEN DAYOFWEEK(pickup_datetime) NOT IN (0, 6) -- weekday
             AND HOUR(pickup_datetime) BETWEEN 5 AND 20
             THEN 'Peak (Weekday 5am-9pm)'
        ELSE 'Off-Peak'
    END                                               AS time_category,
    COUNT(CASE WHEN cbd_congestion_fee > 0 THEN 1 END) AS cbd_trips,
    SUM(cbd_congestion_fee)                            AS total_cbd_fees,
    AVG(CASE WHEN cbd_congestion_fee > 0
        THEN cbd_congestion_fee END)                   AS avg_cbd_fee
FROM {{ ref('fct_taxi_trips') }}
GROUP BY 1, 2, 3
```

### 6.12 Marts Revenue — `met_daily_revenue.sql`

```sql
{{
  config(
    materialized='table'
  )
}}

SELECT
    DATE_TRUNC('day', pickup_datetime)               AS trip_date,
    taxi_type,
    COUNT(*)                                          AS total_trips,
    SUM(total_amount)                                 AS total_revenue,
    AVG(total_amount)                                 AS avg_revenue_per_trip,
    SUM(total_amount) / NULLIF(SUM(trip_distance), 0) AS revenue_per_mile,
    SUM(fare_amount)                                  AS total_fares,
    SUM(tip_amount)                                   AS total_tips,
    AVG(tip_amount) / NULLIF(AVG(fare_amount), 0) * 100 AS avg_tip_pct,
    SUM(tolls_amount)                                 AS total_tolls,
    SUM(congestion_surcharge)                         AS total_congestion_surcharges,
    SUM(airport_fee)                                  AS total_airport_fees,
    SUM(cbd_congestion_fee)                           AS total_cbd_fees
FROM {{ ref('fct_taxi_trips') }}
GROUP BY 1, 2
```

### 6.13 Marts Revenue — `met_payment_distribution.sql`

```sql
{{
  config(
    materialized='table'
  )
}}

SELECT
    DATE_TRUNC('month', pickup_datetime) AS trip_month,
    taxi_type,
    payment_type_name,
    COUNT(*)                              AS trip_count,
    SUM(total_amount)                     AS total_revenue,
    AVG(tip_amount)                       AS avg_tip,
    AVG(tip_amount) / NULLIF(AVG(fare_amount), 0) * 100 AS avg_tip_pct
FROM {{ ref('fct_taxi_trips') }}
GROUP BY 1, 2, 3
```

### 6.14 Marts Geographic — `met_zone_pickup_ranking.sql`

```sql
{{
  config(
    materialized='table'
  )
}}

SELECT
    DATE_TRUNC('month', pickup_datetime) AS trip_month,
    pickup_borough,
    pickup_zone,
    pu_location_id,
    COUNT(*)                              AS trip_count,
    SUM(total_amount)                     AS total_revenue,
    AVG(total_amount)                     AS avg_revenue_per_trip
FROM {{ ref('fct_taxi_trips') }}
GROUP BY 1, 2, 3, 4
```

### 6.15 Marts Operations — `met_hourly_demand.sql`

```sql
{{
  config(
    materialized='table'
  )
}}

SELECT
    DATE_TRUNC('day', pickup_datetime)                  AS trip_date,
    HOUR(pickup_datetime)                                AS pickup_hour,
    DAYOFWEEK(pickup_datetime)                           AS day_of_week,
    taxi_type,
    COUNT(*)                                              AS trip_count,
    AVG(DATEDIFF('minute', pickup_datetime, dropoff_datetime)) AS avg_duration_min,
    AVG(trip_distance)                                    AS avg_distance,
    AVG(trip_distance / NULLIF(
        DATEDIFF('minute', pickup_datetime, dropoff_datetime) / 60.0, 0
    ))                                                    AS avg_speed_mph
FROM {{ ref('fct_taxi_trips') }}
WHERE DATEDIFF('minute', pickup_datetime, dropoff_datetime) > 0
GROUP BY 1, 2, 3, 4
```

---

## 7. Step 5 — dbt Seeds & Snapshots

### 7.1 Seeds

**`seeds/rate_codes.csv`**
```csv
rate_code_id,rate_code_name
1,Standard rate
2,JFK
3,Newark
4,Nassau or Westchester
5,Negotiated fare
6,Group ride
```

**`seeds/payment_types.csv`**
```csv
payment_type_id,payment_type_name
1,Credit card
2,Cash
3,No charge
4,Dispute
5,Unknown
6,Voided trip
```

**`seeds/vendor_lookup.csv`**
```csv
vendor_id,vendor_name
1,Creative Mobile Technologies (CMT)
2,VeriFone Inc. (VTS)
6,Novatel
```

### 7.2 Snapshot — `snap_taxi_zone_lookup.sql`

```sql
{% snapshot snap_taxi_zone_lookup %}

{{
  config(
    target_schema='snapshots',
    unique_key='LocationID',
    strategy='check',
    check_cols=['Borough', 'Zone', 'service_zone'],
    dbt_valid_to_current="'9999-12-31'::timestamp_ntz",
    invalidate_hard_deletes=True
  )
}}

SELECT * FROM {{ source('tlc_reference', 'taxi_zone_lookup') }}

{% endsnapshot %}
```

---

## 8. Step 6 — Data Quality & Testing

### 8.1 Quality Layers Overview

```
Layer 1 — Code Style:       SQLFluff + dbt-checkpoint (pre-commit)
Layer 2 — Project Structure: dbt_project_evaluator (CI)
Layer 3 — Data Quality:     dbt tests (generic, dbt-expectations, singular)
Layer 4 — Observability:    Elementary anomaly detection (inline, same DAG)
Layer 5 — Governance:       Contracts, Access, Groups, Versions
```

### 8.2 Model Tests — `models/marts/core/_core__models.yml`

```yaml
version: 2

groups:
  - name: tlc_analytics
    owner:
      name: "Taime"
      email: "taime@mcsedition.com"

models:
  - name: fct_taxi_trips
    group: tlc_analytics
    access: public
    config:
      contract:
        enforced: true
    latest_version: 1
    versions:
      - v: 1
    columns:
      - name: trip_surrogate_key
        data_type: varchar
        tests:
          - unique
          - not_null
      - name: pickup_datetime
        data_type: timestamp_ntz
        tests:
          - not_null
      - name: fare_amount
        data_type: float
        tests:
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
              max_value: 1000
      - name: trip_distance
        data_type: float
        tests:
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
              max_value: 500
      - name: pu_location_id
        data_type: integer
        tests:
          - relationships:
              to: ref('dim_zones')
              field: location_id
      - name: passenger_count
        data_type: float
        tests:
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
              max_value: 9
      - name: vendor_id
        data_type: integer
        tests:
          - accepted_values:
              values: [1, 2, 6]

    # Elementary observability (inline, same DAG)
    tests:
      - elementary.volume_anomalies:
          timestamp_column: pickup_datetime
          where_expression: "pickup_datetime >= DATEADD(month, -3, CURRENT_DATE)"
      - elementary.schema_changes
```

### 8.3 Generic Custom Test — `tests/generic/test_positive_value.sql`

```sql
{% test positive_value(model, column_name) %}
SELECT *
FROM {{ model }}
WHERE {{ column_name }} < 0
{% endtest %}
```

### 8.4 Singular Tests

**`tests/singular/assert_no_future_trips.sql`**
```sql
SELECT *
FROM {{ ref('fct_taxi_trips') }}
WHERE pickup_datetime > CURRENT_TIMESTAMP()
```

**`tests/singular/assert_fare_consistent_with_distance.sql`**
```sql
-- Trips > 5 miles with $0 fare on standard rate are suspicious
SELECT *
FROM {{ ref('fct_taxi_trips') }}
WHERE trip_distance > 5
  AND fare_amount = 0
  AND rate_code_id = 1
```

### 8.5 Exposures — `models/marts/_exposures.yml`

```yaml
version: 2

exposures:
  - name: dashboard_executive_overview
    type: application
    maturity: high
    url: "https://tlc-dashboards.streamlit.app/Executive_Overview"
    description: "Executive summary with north star KPIs"
    depends_on:
      - ref('met_executive_summary')
    owner:
      name: Taime
      email: taime@mcsedition.com

  - name: dashboard_revenue_analysis
    type: application
    maturity: high
    url: "https://tlc-dashboards.streamlit.app/Revenue"
    description: "Revenue & fare analysis"
    depends_on:
      - ref('met_daily_revenue')
      - ref('met_revenue_by_zone')
      - ref('met_revenue_by_rate_code')
      - ref('met_payment_distribution')
    owner:
      name: Taime
      email: taime@mcsedition.com

  - name: dashboard_geographic_intel
    type: application
    maturity: medium
    url: "https://tlc-dashboards.streamlit.app/Geographic"
    description: "Zone intelligence and spatial analysis"
    depends_on:
      - ref('met_zone_pickup_ranking')
      - ref('met_zone_pair_analysis')
      - ref('met_borough_summary')
      - ref('met_airport_trips')
    owner:
      name: Taime
      email: taime@mcsedition.com

  - name: dashboard_congestion_pricing
    type: application
    maturity: high
    url: "https://tlc-dashboards.streamlit.app/Congestion_Pricing"
    description: "CBD congestion pricing impact analysis (NEW 2025-2026)"
    depends_on:
      - ref('met_cbd_daily_impact')
      - ref('met_cbd_vs_non_cbd')
      - ref('met_cbd_peak_offpeak')
      - ref('met_cbd_yellow_vs_green')
    owner:
      name: Taime
      email: taime@mcsedition.com

  - name: dashboard_operations
    type: application
    maturity: medium
    url: "https://tlc-dashboards.streamlit.app/Operations"
    description: "Operational performance and demand patterns"
    depends_on:
      - ref('met_hourly_demand')
      - ref('met_trip_duration_dist')
      - ref('met_avg_speed_trend')
      - ref('met_vendor_performance')
    owner:
      name: Taime
      email: taime@mcsedition.com

  - name: grafana_finops_dashboard
    type: dashboard
    maturity: high
    url: "https://your-org.grafana.net/d/snowflake-finops"
    description: "Snowflake FinOps monitoring — credit burn, warehouse metrics, budget alerts"
    depends_on: []
    owner:
      name: Taime
      email: taime@mcsedition.com
```

---

## 9. Step 7 — CI/CD Pipeline

### 9.1 SQLFluff Config — `.sqlfluff`

```ini
[sqlfluff]
dialect = snowflake
templater = dbt
max_line_length = 120
exclude_rules = LT05

[sqlfluff:indentation]
indent_unit = space
tab_space_size = 4

[sqlfluff:rules:capitalisation.keywords]
capitalisation_policy = upper

[sqlfluff:rules:capitalisation.functions]
capitalisation_policy = upper

[sqlfluff:rules:capitalisation.literals]
capitalisation_policy = upper

[sqlfluff:rules:convention.terminator]
multiline_newline = true
```

### 9.2 Pre-commit — `.pre-commit-config.yaml`

```yaml
repos:
  - repo: https://github.com/dbt-checkpoint/dbt-checkpoint
    rev: v2.0.0
    hooks:
      - id: check-model-has-description
      - id: check-model-has-tests
        args: ["--test-cnt", "1"]
      - id: check-column-name-contract
      - id: check-model-parents-schema
```

### 9.3 GitHub Actions — `.github/workflows/ci.yml`

```yaml
name: dbt CI

on:
  pull_request:
    branches: [main]

jobs:
  lint-and-test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: |
          pip install dbt-snowflake sqlfluff sqlfluff-templater-dbt

      - name: SQLFluff Lint
        run: sqlfluff lint models/ --dialect snowflake

      - name: dbt deps
        run: dbt deps
        env:
          DBT_PROFILES_DIR: .

      - name: dbt build (modified only)
        run: dbt build --select state:modified+ --defer --state target-base/
        env:
          DBT_PROFILES_DIR: .
          SNOWFLAKE_ACCOUNT: ${{ secrets.SNOWFLAKE_ACCOUNT }}
          SNOWFLAKE_USER: ${{ secrets.SNOWFLAKE_USER }}
          SNOWFLAKE_PASSWORD: ${{ secrets.SNOWFLAKE_PASSWORD }}

      - name: dbt_project_evaluator
        run: dbt test --select package:dbt_project_evaluator
```

### 9.4 CI Pipeline Flow

```
PR opened on GitHub
│
├─ Step 1: pre-commit (dbt-checkpoint)
│   └─ "Every model has a description? Columns documented?"
│
├─ Step 2: SQLFluff lint
│   └─ "SQL follows the style guide?"
│
├─ Step 3: dbt build --select state:modified+
│   └─ "Code compiles? Tests pass?"
│
├─ Step 4: dbt_project_evaluator
│   └─ "DAG follows best practices?"
│
└─ ✅ PR mergeable
```

---

## 10. Step 8 — Streamlit Dashboards

### 10.1 Project Structure

```
streamlit_app/
├── app.py                          # Entry point + navigation
├── requirements.txt
├── .streamlit/
│   └── secrets.toml                # Snowflake credentials
├── utils/
│   └── snowflake_conn.py           # Connection manager (st.connection)
└── pages/
    ├── 1_Executive_Overview.py     # Dashboard 1
    ├── 2_Revenue_Analysis.py       # Dashboard 2
    ├── 3_Geographic_Intel.py       # Dashboard 3
    ├── 4_Congestion_Pricing.py     # Dashboard 4
    └── 5_Operations.py             # Dashboard 5
```

### 10.2 `requirements.txt`

```
streamlit>=1.35.0
plotly>=5.20.0
snowflake-connector-python>=3.8.0
pandas>=2.2.0
```

### 10.3 Dashboard → Mart Mapping

| Streamlit Page | dbt Mart Schema | Models Queried |
|---|---|---|
| `1_Executive_Overview.py` | `executive` | `met_executive_summary` |
| `2_Revenue_Analysis.py` | `revenue` | `met_daily_revenue`, `met_revenue_by_zone`, `met_revenue_by_rate_code`, `met_payment_distribution` |
| `3_Geographic_Intel.py` | `geographic` | `met_zone_pickup_ranking`, `met_zone_pair_analysis`, `met_borough_summary`, `met_airport_trips` |
| `4_Congestion_Pricing.py` | `congestion` | `met_cbd_daily_impact`, `met_cbd_vs_non_cbd`, `met_cbd_peak_offpeak`, `met_cbd_yellow_vs_green` |
| `5_Operations.py` | `operations` | `met_hourly_demand`, `met_trip_duration_dist`, `met_avg_speed_trend`, `met_vendor_performance` |

### 10.4 Example Page — `4_Congestion_Pricing.py`

```python
import streamlit as st
import plotly.express as px

st.set_page_config(page_title="Congestion Pricing Impact", layout="wide")
st.title("🚕 Congestion Pricing Impact (CBD)")

conn = st.connection("snowflake")

# Query pre-aggregated mart tables (lightweight, preserves Snowflake credits)
df_cbd = conn.query("SELECT * FROM ANALYTICS.CONGESTION.met_cbd_daily_impact ORDER BY trip_date")
df_compare = conn.query("SELECT * FROM ANALYTICS.CONGESTION.met_cbd_vs_non_cbd")
df_peak = conn.query("SELECT * FROM ANALYTICS.CONGESTION.met_cbd_peak_offpeak")

# --- KPI Cards ---
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total CBD Fees", f"${df_cbd['TOTAL_CBD_FEES'].sum():,.0f}")
col2.metric("% Trips in CBD", f"{df_cbd['CBD_TRIP_PCT'].mean():.1f}%")
col3.metric("Avg CBD Fee/Trip", f"${df_cbd['AVG_CBD_FEE'].mean():.2f}")
col4.metric("Avg Speed (CBD)", f"{df_compare[df_compare['ZONE_CATEGORY']=='CBD Zone']['AVG_SPEED_MPH'].mean():.1f} mph")

# --- CBD Trip Volume Trend ---
fig1 = px.line(
    df_cbd, x="TRIP_DATE", y="CBD_TRIPS", color="TAXI_TYPE",
    title="CBD Trip Volume Trend (Daily)",
    labels={"CBD_TRIPS": "Trips in CBD", "TRIP_DATE": "Date"}
)
st.plotly_chart(fig1, use_container_width=True)

# --- CBD vs Non-CBD Comparison ---
fig2 = px.bar(
    df_compare, x="TRIP_MONTH", y="AVG_REVENUE_PER_TRIP",
    color="ZONE_CATEGORY", barmode="group",
    facet_col="TAXI_TYPE",
    title="Avg Revenue per Trip: CBD vs Non-CBD"
)
st.plotly_chart(fig2, use_container_width=True)

# --- Peak vs Off-Peak ---
fig3 = px.bar(
    df_peak, x="TRIP_MONTH", y="TOTAL_CBD_FEES",
    color="TIME_CATEGORY",
    title="CBD Fees: Peak vs Off-Peak",
    barmode="stack"
)
st.plotly_chart(fig3, use_container_width=True)
```

### 10.5 Deployment

```bash
# Deploy to Streamlit Community Cloud (free, public)
# 1. Push streamlit_app/ to GitHub
# 2. Connect repo on share.streamlit.io
# 3. Set secrets (Snowflake credentials) in Streamlit Cloud dashboard
```

---

## 11. Step 9 — Grafana FinOps Dashboard

### 11.1 Setup

1. **Grafana Cloud** (free trial): `https://grafana.com/products/cloud/`
2. **Install Snowflake Data Source Plugin**: Grafana → Connections → Add Data Source → Snowflake
3. **Configure connection**: Account, warehouse (`TLC_WH`), database (`SNOWFLAKE`), schema (`ACCOUNT_USAGE`), role (`ACCOUNTADMIN`)

### 11.2 FinOps KPIs — SQL Queries for Grafana Panels

#### Panel 1: Credit Burn Rate (Daily)

```sql
SELECT
    USAGE_DATE,
    SUM(CREDITS_USED) AS credits_used,
    SUM(CREDITS_USED) * 3.50 AS estimated_cost_usd
FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_DAILY_HISTORY
WHERE USAGE_DATE >= DATEADD('day', -30, CURRENT_DATE())
GROUP BY USAGE_DATE
ORDER BY USAGE_DATE;
```

#### Panel 2: Cumulative Credit Consumption vs Budget

```sql
SELECT
    USAGE_DATE,
    SUM(CREDITS_USED) OVER (ORDER BY USAGE_DATE) AS cumulative_credits,
    SUM(CREDITS_USED) OVER (ORDER BY USAGE_DATE) * 3.50 AS cumulative_cost_usd,
    100.0 AS budget_credits,  -- adjust to your credit allocation
    350.0 AS budget_usd       -- 400$ total, 50$ safety margin
FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_DAILY_HISTORY
ORDER BY USAGE_DATE;
```

#### Panel 3: Credits by Warehouse

```sql
SELECT
    WAREHOUSE_NAME,
    SUM(CREDITS_USED) AS total_credits,
    SUM(CREDITS_USED) * 3.50 AS estimated_cost_usd
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE START_TIME >= DATEADD('day', -30, CURRENT_DATE())
GROUP BY WAREHOUSE_NAME
ORDER BY total_credits DESC;
```

#### Panel 4: Top 10 Most Expensive Queries

```sql
SELECT
    QUERY_ID,
    QUERY_TEXT,
    WAREHOUSE_NAME,
    EXECUTION_STATUS,
    TOTAL_ELAPSED_TIME / 1000 AS duration_seconds,
    BYTES_SCANNED / (1024*1024*1024) AS gb_scanned,
    CREDITS_USED_CLOUD_SERVICES
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE START_TIME >= DATEADD('day', -7, CURRENT_DATE())
ORDER BY TOTAL_ELAPSED_TIME DESC
LIMIT 10;
```

#### Panel 5: Storage Usage Trend

```sql
SELECT
    USAGE_DATE,
    AVERAGE_STAGE_BYTES / (1024*1024*1024*1024) AS stage_tb,
    AVERAGE_DATABASE_BYTES / (1024*1024*1024*1024) AS database_tb,
    AVERAGE_FAILSAFE_BYTES / (1024*1024*1024*1024) AS failsafe_tb
FROM SNOWFLAKE.ACCOUNT_USAGE.STORAGE_USAGE
WHERE USAGE_DATE >= DATEADD('day', -30, CURRENT_DATE())
ORDER BY USAGE_DATE;
```

#### Panel 6: Warehouse Idle Time

```sql
SELECT
    WAREHOUSE_NAME,
    SUM(CREDITS_USED) AS total_credits,
    SUM(CREDITS_USED_COMPUTE) AS compute_credits,
    SUM(CREDITS_USED_CLOUD_SERVICES) AS cloud_credits,
    ROUND(SUM(CREDITS_USED_CLOUD_SERVICES) / NULLIF(SUM(CREDITS_USED_COMPUTE), 0) * 100, 2) AS cloud_services_pct
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE START_TIME >= DATEADD('day', -30, CURRENT_DATE())
GROUP BY WAREHOUSE_NAME;
-- ALERT: cloud_services_pct > 10% means you're paying for cloud services
```

### 11.3 Grafana Alerts

| Alert | Condition | Severity |
|---|---|---|
| Burn rate élevé | > 20 credits/day (~$5/day) | Warning |
| Budget 50% atteint | Cumulative > 200$ | Warning |
| Budget 80% atteint | Cumulative > 320$ | Critical |
| Query > 5 min | Duration > 300s | Warning |
| Warehouse idle > 30 min | Check auto-suspend config | Info |

---

## Appendix A — TLC Reference Data

### Key Zones

| LocationID | Borough | Zone | Notes |
|---|---|---|---|
| 132 | Queens | JFK Airport | Flat rate $70 (rate_code 2) |
| 138 | Queens | LaGuardia Airport | Metered |
| 161 | Manhattan | Midtown Center | Highest volume zone |
| 264 | Unknown | Unknown | **Filter out** |
| 265 | Unknown | Unknown | **Filter out** |
| 56, 57 | — | — | **Duplicate borough/zone** — watch aggregations |

### Rate Codes

| ID | Name |
|---|---|
| 1 | Standard rate |
| 2 | JFK (flat $70) |
| 3 | Newark |
| 4 | Nassau or Westchester |
| 5 | Negotiated fare |
| 6 | Group ride |

### Payment Types

| ID | Name | Notes |
|---|---|---|
| 1 | Credit card | Tips recorded (~66% of trips) |
| 2 | Cash | Tips NOT recorded (~32%) |
| 3 | No charge | — |
| 4 | Dispute | — |
| 5 | Unknown | — |
| 6 | Voided trip | — |

### CBD Congestion Pricing (effective January 2025)

- **Fee**: ~$0.75 per taxi trip entering Manhattan south of 60th Street
- **When**: Peak hours (weekdays 5am-9pm, weekends 9am-9pm)
- **Field**: `cbd_congestion_fee` in trip data
- **Impact**: +19% yellow taxi trips in January 2025 vs prior year

---

## Appendix B — KPI Catalog

### North Star KPIs (Top 10)

| # | KPI | Category | Calculation |
|---|---|---|---|
| 1 | Total Trips | Volume | `COUNT(*)` |
| 2 | Total Revenue | Financial | `SUM(total_amount)` |
| 3 | Revenue per Mile | Efficiency | `SUM(total_amount) / SUM(trip_distance)` |
| 4 | Avg Tip % | Satisfaction | `AVG(tip_amount) / AVG(fare_amount) * 100` |
| 5 | Top Pickup Zones | Geographic | `COUNT(*) GROUP BY pickup_zone ORDER BY DESC` |
| 6 | CBD Fee Revenue | Congestion | `SUM(cbd_congestion_fee)` |
| 7 | % Trips in CBD | Congestion | `COUNT(cbd > 0) / COUNT(*)` |
| 8 | Avg Trip Speed | Operational | `AVG(distance / duration_hours)` |
| 9 | Peak Hour Distribution | Demand | `COUNT(*) GROUP BY HOUR(pickup)` |
| 10 | Yellow vs Green Split | Segmentation | `COUNT(*) GROUP BY taxi_type` |

### FinOps KPIs (Snowflake)

| KPI | Source View | Alert Threshold |
|---|---|---|
| Credits consumed (daily) | `METERING_DAILY_HISTORY` | > 20 credits/day |
| Credits remaining vs budget | Cumulative calculation | < 20% remaining |
| Projection burn date | Trend extrapolation | < 7 days remaining |
| Compute vs cloud services ratio | `WAREHOUSE_METERING_HISTORY` | Cloud services > 10% |
| Top expensive queries | `QUERY_HISTORY` | Duration > 300s |
| Storage per database | `DATABASE_STORAGE_USAGE_HISTORY` | > 1TB |
| Warehouse utilization | `WAREHOUSE_METERING_HISTORY` | Idle > 50% |