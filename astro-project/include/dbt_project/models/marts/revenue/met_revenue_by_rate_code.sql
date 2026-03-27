{{
  config(
    materialized='table'
  )
}}

SELECT
    DATE_TRUNC('month', pickup_datetime) AS trip_month,
    taxi_type,
    rate_code_id,
    rate_code_name,
    COUNT(*) AS trip_count,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS avg_revenue_per_trip,
    AVG(trip_distance) AS avg_trip_distance
FROM {{ ref('fct_taxi_trips') }}
GROUP BY 1, 2, 3, 4
