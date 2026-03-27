{{
  config(
    materialized='table'
  )
}}

SELECT
    DATE_TRUNC('month', pickup_datetime) AS trip_month,
    taxi_type,
    pickup_borough,
    pickup_zone,
    pu_location_id,
    COUNT(*) AS trip_count,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS avg_revenue_per_trip,
    SUM(tip_amount) AS total_tips,
    AVG(tip_amount) / NULLIF(AVG(fare_amount), 0) * 100 AS avg_tip_pct
FROM {{ ref('fct_taxi_trips') }}
GROUP BY 1, 2, 3, 4, 5
