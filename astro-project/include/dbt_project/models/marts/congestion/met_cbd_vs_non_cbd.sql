{{
  config(
    materialized='table'
  )
}}

SELECT
    DATE_TRUNC('month', pickup_datetime) AS trip_month,
    taxi_type,
    CASE WHEN cbd_congestion_fee > 0 THEN 'CBD Zone' ELSE 'Non-CBD Zone' END AS zone_category,
    COUNT(*) AS total_trips,
    AVG(total_amount) AS avg_revenue_per_trip,
    SUM(total_amount) / NULLIF(SUM(trip_distance), 0) AS revenue_per_mile,
    AVG(tip_amount) / NULLIF(AVG(fare_amount), 0) * 100 AS avg_tip_pct,
    AVG(trip_distance / NULLIF(
        DATEDIFF('minute', pickup_datetime, dropoff_datetime) / 60.0, 0
    )) AS avg_speed_mph,
    AVG(DATEDIFF('minute', pickup_datetime, dropoff_datetime)) AS avg_duration_min
FROM {{ ref('fct_taxi_trips') }}
GROUP BY 1, 2, 3
