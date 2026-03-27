{{
  config(
    materialized='table'
  )
}}

SELECT
    DATE_TRUNC('month', pickup_datetime) AS trip_month,
    vendor_id,
    vendor_name,
    taxi_type,
    COUNT(*)                              AS trip_count,
    SUM(total_amount)                     AS total_revenue,
    AVG(total_amount)                     AS avg_revenue_per_trip,
    AVG(trip_distance)                    AS avg_trip_distance,
    AVG(tip_amount) / NULLIF(AVG(fare_amount), 0) * 100 AS avg_tip_pct,
    AVG(DATEDIFF('minute', pickup_datetime, dropoff_datetime)) AS avg_duration_min
FROM {{ ref('fct_taxi_trips') }}
GROUP BY 1, 2, 3, 4
