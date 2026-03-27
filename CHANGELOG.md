# Changelog

All notable changes to the `snow-airflow-dbt` project will be documented in this file.

## [Unreleased]

### 2026-03-27 — Grafana FinOps Dashboard (spec Section 11)

- Created `grafana/` directory with infrastructure-as-code provisioning files
- `datasource.json` — Snowflake datasource config for Grafana Cloud (`grafana-snowflake-datasource` plugin v1.14.12)
  - Connects to `SNOWFLAKE.ACCOUNT_USAGE` with `ACCOUNTADMIN` role, `TLC_WH` warehouse
  - Password auth (placeholder `__SNOWFLAKE_PASSWORD__` replaced at deploy time)
- `finops-dashboard.json` — Full Grafana dashboard (6 panels):
  - Panel 1: **Credit Burn Rate** (daily bar chart + USD cost line, dual-axis)
  - Panel 2: **Cumulative Credits vs Budget** (running total with $350 budget threshold dashed line)
  - Panel 3: **Credits by Warehouse** (donut pie chart)
  - Panel 4: **Top 10 Expensive Queries** (table with gauge for duration, last 7 days)
  - Panel 5: **Storage Usage Trend** (stacked area: database/stage/failsafe in GB)
  - Panel 6: **Warehouse Credit Breakdown** (stacked bar: compute vs cloud services + % line)
- `deploy.sh` — Automated provisioning script (idempotent: create or update)
  - Checks Snowflake plugin is installed
  - Creates/updates datasource via Grafana API
  - Deploys dashboard via `/api/dashboards/db`
  - Requires: `GRAFANA_URL`, `GRAFANA_TOKEN` (Service Account), `SNOWFLAKE_PASSWORD`
- Target Grafana Cloud instance: `https://stefentaime.grafana.net` (v13.0.0, Enterprise)
- All queries use `SNOWFLAKE.ACCOUNT_USAGE` views (METERING_DAILY_HISTORY, WAREHOUSE_METERING_HISTORY, QUERY_HISTORY, STORAGE_USAGE)
- **Deployed to Grafana Cloud:**
  - Installed `grafana-snowflake-datasource` plugin v1.15.0 via API (`POST /api/plugins/.../install`)
  - Created datasource `Snowflake - FinOps` (UID: `afhbjh3vp730gd`)
  - Deployed 6-panel FinOps dashboard: `https://stefentaime.grafana.net/d/snowflake-finops/snowflake-finops-credit-and-cost-monitoring`
- **Grafana Alerts (spec 11.3)** — 5 alert rules deployed via provisioning API:
  - `High Credit Burn Rate` (>20 credits/day) — severity: warning
  - `Budget 50% Reached` (>$200 cumulative) — severity: warning
  - `Budget 80% Reached` (>$320 cumulative) — severity: critical
  - `Long Running Query` (>5 min / 300s) — severity: warning
  - `Warehouse Auto-Suspend Check` (cloud services >10%) — severity: info
  - Alert folder: `Snowflake FinOps Alerts`, rule group: `Snowflake FinOps`
  - Exported to `grafana/alert-rules.json` for infrastructure-as-code

### 2026-03-27 — Streamlit Dashboards (spec Section 10)

- Created `streamlit_app/` directory with full multi-page Plotly dashboard application
- Structure: `app.py` (entry point with `st.navigation`), `utils/snowflake_conn.py`, 5 dashboard pages
- Configured `.streamlit/config.toml` with dark theme (primary: #FF6B35, bg: #0E1117)
- Configured `.streamlit/secrets.toml` for `st.connection("snowflake")` (excluded from git via `**/secrets.toml`)
- Connection manager uses `st.cache_resource` + `conn.query(ttl=600)` for cached queries
- Page 1 — **Executive Overview**: KPI cards (6 metrics), dual-axis bar+line for monthly trips/revenue, donut chart for taxi type split, avg fare trend, CBD adoption chart
- Page 2 — **Revenue Analysis**: daily revenue area chart, stacked revenue component breakdown (fares/tips/tolls/surcharges/CBD), top 15 zones horizontal bar with colorscale, rate code donut, payment distribution 100% stacked bar
- Page 3 — **Geographic Intel**: top 20 pickup zones (Turbo colorscale), borough trip trend lines, avg rev/trip by borough, top 15 route corridors with Viridis color-encoded fare, airport sunburst + trend
- Page 4 — **Congestion Pricing**: CBD daily impact dual-axis, CBD vs Non-CBD faceted grouped bars (revenue + speed), peak vs off-peak stacked fees, yellow vs green CBD penetration
- Page 5 — **Operations**: hourly demand heatmap (Inferno), duration distribution grouped bars, fare vs distance by duration bucket, daily speed trend with NYC avg reference line, vendor radar chart, vendor summary table
- All charts use `plotly_dark` template with transparent backgrounds, professional color palettes, and responsive layouts
- Queries target pre-aggregated dbt mart tables only (16 models across 5 schemas: EXECUTIVE, REVENUE, GEOGRAPHIC, CONGESTION, OPERATIONS)
- Tested locally: `streamlit run app.py` on port 8502 — HTTP 200, health check OK
- Requirements: `streamlit>=1.41.0`, `plotly>=5.24.0`, `snowflake-connector-python[pandas]>=3.12.0`, `pandas>=2.2.0`

### 2026-03-27 — Astronomer Setup

- Installed Astro CLI v1.40.1 via Homebrew (macOS)
- Configured Docker as container runtime (`astro config set container.binary docker -g`)
- Initialized Astronomer project in `astro-project/` with Astro Runtime 3.1 (Airflow 3)
- Added `apache-airflow-providers-snowflake>=6.3.0` to requirements.txt
- Added `astronomer-cosmos>=1.5.0` to requirements.txt (dbt integration)
- Created `include/sql/` directory with COPY INTO SQL files:
  - `copy_into_yellow.sql` — Yellow taxi trips (Parquet, MATCH_BY_COLUMN_NAME)
  - `copy_into_green.sql` — Green taxi trips (Parquet, MATCH_BY_COLUMN_NAME)
  - `copy_into_zone_lookup.sql` — Taxi zone lookup (CSV)
- Configured `airflow_settings.yaml` with Snowflake connection placeholder
- Added Airflow variables: `tlc_base_url`, `tlc_data_months`
- Removed default example DAG
- Verified `astro dev start` — 5 containers running (Airflow 3 architecture):
  - api-server (port 8080), scheduler, dag-processor, triggerer, postgres (port 5432)
- Airflow UI accessible at: http://astro-project.localhost:6563
- Snowflake connection (`snowflake_default`) auto-created from airflow_settings.yaml

### 2026-03-27 — Snowflake Setup (spec Section 3)

- Connected to Snowflake account `KYEUCFS-BQ14035` (AWS, Enterprise edition)
- Created warehouse `TLC_WH` (X-SMALL, auto-suspend 60s, auto-resume, initially suspended)
- Created databases: `RAW`, `ANALYTICS`
- Created schemas: `RAW.TLC_TRIPS`, `RAW.TLC_REFERENCE`
- Created raw tables:
  - `RAW.TLC_TRIPS.yellow_taxi_trips` (21 columns incl. `cbd_congestion_fee`)
  - `RAW.TLC_TRIPS.green_taxi_trips` (21 columns incl. `cbd_congestion_fee`)
  - `RAW.TLC_REFERENCE.taxi_zone_lookup` (5 columns)
- Created internal stages: `tlc_stage`, `tlc_ref_stage`
- Created file formats: `parquet_format` (Parquet), `csv_format` (CSV, skip header)
- Enabled `SCHEMA_EVOLUTION = TRUE` on yellow and green taxi trip tables
- Created resource monitor `tlc_budget_monitor`:
  - Credit quota: 100/month
  - 50% notify, 80% notify, 95% suspend, 100% suspend immediate
  - Assigned to warehouse `TLC_WH`

### 2026-03-27 — Airflow DAGs (spec Section 4)

- Implemented DAG 1: `tlc_raw_ingestion.py`
  - TaskGroup `trip_data`: download yellow/green Parquet -> PUT to stage -> COPY INTO
  - TaskGroup `reference_data`: download zone lookup CSV -> PUT to stage -> COPY INTO
  - Cleanup staged files after load
  - TriggerDagRunOperator to launch DAG 2 on success
  - Schedule: `@monthly`, configurable months via Airflow variable `tlc_data_months`
  - Uses `SQLExecuteQueryOperator` (replaces deprecated SnowflakeOperator in provider v6.11.0)
- Implemented DAG 2: `tlc_dbt_transform.py`
  - `dbt source freshness` -> `dbt seed` -> `dbt build` -> `elementary report`
  - Schedule: `None` (triggered by DAG 1)
  - Uses BashOperator for dbt CLI commands
- Validated both DAGs: `astro dev parse` — 2 passed, 0 errors
- Airflow 3 imports: `airflow.sdk.DAG`, `airflow.sdk.task_group`
- Snowflake provider v6.11.0: `SQLExecuteQueryOperator` (not deprecated `SnowflakeOperator`)
- Cosmos v1.13.1 installed (available for future model-level observability upgrade)

### 2026-03-27 — DAG 1 Execution & Fixes

- **Bug fix**: `TemplateNotFound` — SQL files not found by Jinja template engine
  - Root cause: absolute paths in `sql=` param; Airflow only searches `dags/` by default
  - Fix: added `template_searchpath=[INCLUDE_SQL_DIR]` to DAG, use relative filenames
- **Bug fix**: Parquet timestamps corrupted (year 56M instead of 2026)
  - Root cause: TLC Parquet uses INT96/nanosecond timestamps; Snowflake defaults to microseconds
  - Fix: recreated `parquet_format` with `USE_LOGICAL_TYPE = TRUE`
- **Fix**: `TriggerDagRunOperator` deprecated import path updated to `airflow.providers.standard`
- DAG 1 executed successfully — all 10 tasks passed:
  - download (2 tasks) -> stage (3 tasks) -> copy (3 tasks) -> cleanup -> trigger
  - Total runtime: ~80 seconds
- Data loaded into Snowflake:
  - `yellow_taxi_trips`: 3,724,889 rows (January 2026)
  - `green_taxi_trips`: 40,272 rows (January 2026)
  - `taxi_zone_lookup`: 265 rows
  - Avg yellow fare: $29.18, CBD congestion fees: $1,935,611 total
  - Avg green fare: $24.19

### 2026-03-27 — Slack Alerting Integration

- Created `include/slack_alerts.py` — custom Slack notification module
  - Uses **Block Kit** (nested inside attachments for color sidebar) via `requests.post`
  - No dependency on `apache-airflow-providers-slack` (Airflow 3 bug #50754, slim image)
  - Block structure: `header` (emoji + title) → `section.fields` (label/value columns) → optional `section` (error code block) → `context` (pipeline footer)
  - Blocks nested inside `attachments[].blocks` to preserve colored sidebar (green/red/orange)
  - `fallback` field included for accessibility (text-only clients, notifications)
  - Callbacks: `on_dag_success` (green `#36a64f`), `on_dag_failure` (red `#ff0000`), `on_task_failure` (orange `#ff9900`), `send_custom_alert` (blue `#439FE0`)
  - Webhook URL read from Airflow Variable `slack_webhook_url`
- Added `AIRFLOW_VAR_SLACK_WEBHOOK_URL` and `AIRFLOW_VAR_SLACK_CHANNEL` to `.env`
  - `AIRFLOW_VAR_` prefix auto-creates Airflow variables on container start
- Integrated Slack callbacks into DAG 1 (`tlc_raw_ingestion`):
  - `on_success_callback=on_dag_success` (DAG-level success)
  - `on_failure_callback=on_dag_failure` (DAG-level failure)
  - `on_failure_callback=on_task_failure` in `default_args` (task-level failure)
- Integrated Slack callbacks into DAG 2 (`tlc_dbt_transform`) — same pattern
- Import method: `sys.path.insert(0, include/)` to make `slack_alerts` importable from DAGs
- Validated: `astro dev parse` — 2 DAGs passed, 0 errors
- Restarted Airflow to load new `.env` variables
- Tested: sent all 3 Block Kit message types from container — delivered to `#macie-finds`

### 2026-03-27 — dbt Project Structure & Models (spec Sections 5+6)

- Added `dbt-core>=1.9.0`, `dbt-snowflake>=1.9.0`, `elementary-data>=0.15.0` to requirements.txt
- Installed in container: dbt-core 1.11.7, dbt-snowflake 1.11.3
- Created dbt project in `include/dbt_project/` (volume-mounted for hot-reload)
  - Moved from `dbt_project/` to `include/dbt_project/` (Astro only mounts `include/`)
  - Updated DAG 2 `DBT_PROJECT_DIR` path accordingly
- **Config files:**
  - `dbt_project.yml` — project config, schema mappings, materialization strategies
  - `profiles.yml` — Snowflake connection via `env_var()` (ANALYTICS database, 4 threads, query_tag)
  - `packages.yml` — dbt_utils 1.3.3, metaplane/dbt_expectations 0.10.10, dbt_project_evaluator 1.2.3, elementary 0.23.0
  - Fixed: `calogica/dbt_expectations` deprecated → `metaplane/dbt_expectations`
- **Seeds (3 CSV):** `rate_codes`, `payment_types`, `vendor_lookup` → schema `reference`
- **Snapshot:** `snap_taxi_zone_lookup` (SCD Type 2, check strategy on Borough/Zone/service_zone)
- **Staging models (3):**
  - `stg_yellow_taxi_trips` — incremental, delete+insert, 3-day lookback, surrogate key
  - `stg_green_taxi_trips` — same pattern (lpep timestamps, taxi_type='green')
  - `stg_taxi_zone_lookup` — table, filters Unknown zones 264/265
  - Added `event_time: pickup_datetime` config for microbatch upstream filtering
- **Intermediate (1):** `int_trips_unioned` — ephemeral union of yellow + green
- **Marts Core (4):**
  - `fct_taxi_trips` — incremental microbatch (daily batches, 3-day lookback, begin 2026-01-01)
  - `dim_zones`, `dim_vendors`, `dim_rate_codes` — table materialization
- **Marts Metrics (16):**
  - Executive (1): `met_executive_summary`
  - Revenue (4): `met_daily_revenue`, `met_revenue_by_zone`, `met_revenue_by_rate_code`, `met_payment_distribution`
  - Geographic (4): `met_zone_pickup_ranking`, `met_zone_pair_analysis`, `met_borough_summary`, `met_airport_trips`
  - Congestion (4): `met_cbd_daily_impact`, `met_cbd_vs_non_cbd`, `met_cbd_peak_offpeak`, `met_cbd_yellow_vs_green`
  - Operations (4): `met_hourly_demand`, `met_trip_duration_dist`, `met_avg_speed_trend`, `met_vendor_performance`
- **YAML configs:** `_sources.yml`, `_staging__models.yml`, `_intermediate__models.yml`, `_core__models.yml`, `_executive__models.yml`, `_revenue__models.yml`, `_geographic__models.yml`, `_congestion__models.yml`, `_operations__models.yml`
- Fixed dbt 1.11 deprecation warnings:
  - `freshness` + `loaded_at_field` moved to `config` block in sources
  - `accepted_values` arguments nested under `arguments` property
- Validated: `dbt deps` — 5 packages installed (0 errors)
- Validated: `dbt debug` — Connection test OK (Snowflake ANALYTICS database)
- Validated: `dbt parse` — all 24 models + 1 snapshot parsed (0 errors)
- Validated: `astro dev parse` — 2 Airflow DAGs passed (0 errors)
- Total: 41 files created (3 config, 3 seeds, 1 snapshot, 24 models, 10 YAML)

### 2026-03-27 — dbt Build & Data Fixes (spec Section 7)

- Ran `dbt seed` — 4 seeds loaded (rate_codes, payment_types, vendor_lookup, plus snapshot)
- Ran `dbt build` — 127 PASS, 11 WARN, 2 ERRORS on first attempt
- **Bug fix**: Surrogate key duplicates in staging models
  - Root cause: TLC data contains true duplicate rows (identical vendor + timestamps + locations)
  - Yellow: 35,722 duplicates; Green: 118 duplicates
  - Fix: Added `ROW_NUMBER() OVER (PARTITION BY 7 columns ORDER BY load_timestamp DESC)` deduplication CTE
  - Expanded surrogate key from 5 to 8 columns (added `fare_amount`, `trip_distance`, `payment_type_id`)
  - Modified: `stg_yellow_taxi_trips.sql`, `stg_green_taxi_trips.sql`
- **Bug fix**: Microbatch strategy caused infinite row duplication in `fct_taxi_trips`
  - Root cause: `int_trips_unioned` is ephemeral — dbt cannot propagate `event_time` through ephemeral models
  - Each daily batch inserted ALL 3.76M rows instead of filtering to the batch date
  - Resulted in 26.3M rows (7x duplication) before detection
  - Fix: Changed `fct_taxi_trips` from `microbatch` to `delete+insert` incremental with 3-day lookback
  - Removed `event_time` config from staging YAML
- **Cleanup**: Zombie Snowflake queries from aborted microbatch continued server-side
  - Multiple DROP TABLE CASCADE + wait cycles to clear corrupted data
  - Verified clean state: 3,764,621 total rows = 3,764,621 distinct surrogate keys
- Final data in Snowflake:
  - `stg_yellow_taxi_trips`: 3,724,350 rows (deduplicated from 3,724,889)
  - `stg_green_taxi_trips`: 40,271 rows (deduplicated from 40,272)
  - `fct_taxi_trips_v1`: 3,764,621 rows (all unique keys)
  - 4 dimension tables, 1 snapshot, 16 metrics models materialized
- Validated: `dbt build` — 127 PASS, 11 WARN, 0 ERROR

### 2026-03-27 — Data Quality & Testing (spec Section 8)

- **Model contracts** on `fct_taxi_trips`:
  - `contract: { enforced: true }` with full column `data_type` declarations (21 columns)
  - `on_schema_change: append_new_columns` (required by contract + incremental)
  - `latest_version: 1` with `versions: [{ v: 1 }]` (model versioning)
- **Groups & access control**:
  - Created group `tlc_analytics` (owner: Taime)
  - All 4 core models: `config: { group: tlc_analytics, access: public }`
  - dbt 1.11 compliance: `access` and `group` in `config` block (not top-level)
- **dbt_expectations tests** (3):
  - `passenger_count`: expect_column_values_to_be_between (0-9)
  - `trip_distance`: expect_column_values_to_be_between (0-500, severity: warn)
  - `fare_amount`: expect_column_values_to_be_between (-100 to 2000, severity: warn)
- **Elementary observability tests** (2):
  - `volume_anomalies` on `pickup_datetime` (last 3 months window, severity: warn)
  - `schema_changes` (severity: warn)
- **Referential integrity test**:
  - `pu_location_id` relationships to `dim_zones.location_id` (severity: warn)
- **Generic test**: `tests/generic/test_positive_value.sql` — reusable test for negative values
- **Singular tests** (2):
  - `assert_no_future_trips.sql` — no pickup_datetime in the future
  - `assert_fare_consistent_with_distance.sql` — flags >5mi trips with $0 fare (severity: warn)
- **Exposures** (`models/marts/_exposures.yml`):
  - 5 Streamlit dashboard exposures (Executive, Revenue, Geographic, Congestion, Operations)
  - 1 Grafana FinOps dashboard exposure
- **dbt 1.11 deprecation fixes**:
  - All generic test parameters nested under `arguments:` property
  - Applies to: `accepted_values`, `relationships`, `dbt_expectations.*`, `elementary.*`
- Data quality findings in real TLC data (set to `warn` severity):
  - `vendor_id`: 1 unexpected value beyond [1, 2, 6]
  - `fare_amount`: 555 outliers outside [-100, 2000]
  - `trip_distance`: 117 outliers outside [0, 500]
  - `fare_consistent_with_distance`: 96 suspicious $0-fare long-distance trips
  - `pu_location_id` relationships: orphaned location IDs (zones 264/265 filtered in dim_zones)
- Validated: `dbt parse` — 0 errors, 0 deprecation warnings
- Validated: `dbt test --select fct_taxi_trips` — PASS=10, WARN=5, ERROR=0
- Validated: `astro dev parse` — 2 DAGs passed, 0 errors
- Files created/modified: `_core__models.yml` (enhanced), `_exposures.yml` (new), `test_positive_value.sql` (new), `assert_no_future_trips.sql` (new), `assert_fare_consistent_with_distance.sql` (new)

### 2026-03-27 — Refactoring: QUALIFY (Snowflake idiom)

- Refactored `stg_yellow_taxi_trips.sql` and `stg_green_taxi_trips.sql`:
  - Replaced CTE `deduplicated` + `WHERE _row_num = 1` with inline `QUALIFY ROW_NUMBER() OVER (...) = 1`
  - Eliminates intermediate CTE — single-pass, Snowflake-idiomatic deduplication
  - Cleaner incremental `WHERE` clause (no longer mixed with `_row_num` filter)
- Validated: `dbt build` — PASS=14, WARN=0, ERROR=0 on both staging models

### 2026-03-27 — CI/CD Pipeline & GitHub Setup (spec Section 9)

- **GitHub repository**: Created private repo `Stefen-Taime/snow-airflow-dbt`
  - Description: "Production-grade ELT pipeline: NYC TLC taxi data -> Snowflake -> dbt -> Streamlit + Grafana FinOps"
  - Topics: snowflake, dbt, airflow, astronomer, streamlit, grafana, elt-pipeline, nyc-tlc, data-engineering, finops
- **GitHub Secrets** (6): `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_ROLE`, `SNOWFLAKE_WAREHOUSE`, `SLACK_WEBHOOK_URL`
- **GitHub Variables** (3): `SNOWFLAKE_DATABASE`, `SNOWFLAKE_SCHEMA`, `DBT_PROFILES_DIR`
- **`.gitignore`**: Comprehensive ignore rules for secrets, Astronomer, dbt (target/, dbt_packages/, .user.yml), Python, OS files, IDE, data files
- **`.sqlfluff`** (`include/dbt_project/.sqlfluff`):
  - Dialect: snowflake, Templater: jinja (with `apply_dbt_builtins = true`)
  - Note: `sqlfluff-templater-dbt 4.1.0` supports up to dbt 1.10, not 1.11 yet — using jinja templater as robust fallback
  - Rules: UPPER keywords/functions/literals, 4-space indent, max 120 chars, exclude LT05
- **`.pre-commit-config.yaml`**:
  - `pre-commit-hooks v5.0.0`: trailing-whitespace, end-of-file-fixer, check-yaml, check-added-large-files
  - `sqlfluff 4.1.0`: sqlfluff-lint on `models/` directory
  - `dbt-checkpoint v1.2.1`: check-model-has-description, check-model-has-tests (min 1), check-column-name-contract
- **`.github/workflows/ci.yml`** — 3-job pipeline:
  - `lint`: SQLFluff lint on models/ (runs on ubuntu-latest, Python 3.11)
  - `build-and-test`: dbt deps -> dbt build (state:modified+, --defer) -> dbt_project_evaluator
  - `dag-validation`: Airflow DAG syntax check (Python import validation)
  - Concurrency: cancel-in-progress per PR
  - Path filter: only triggers on dbt_project/ or workflow changes
- Initial commit and push to GitHub
