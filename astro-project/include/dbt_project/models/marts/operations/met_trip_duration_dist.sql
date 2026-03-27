{{
  config(
    materialized='table'
  )
}}

SELECT
    DATE_TRUNC('month', pickup_datetime) AS trip_month,
    taxi_type,
    CASE
        WHEN DATEDIFF('minute', pickup_datetime, dropoff_datetime) <= 5 THEN '0-5 min'
        WHEN DATEDIFF('minute', pickup_datetime, dropoff_datetime) <= 10 THEN '5-10 min'
        WHEN DATEDIFF('minute', pickup_datetime, dropoff_datetime) <= 20 THEN '10-20 min'
        WHEN DATEDIFF('minute', pickup_datetime, dropoff_datetime) <= 30 THEN '20-30 min'
        WHEN DATEDIFF('minute', pickup_datetime, dropoff_datetime) <= 60 THEN '30-60 min'
        ELSE '60+ min'
    END AS duration_bucket,
    COUNT(*) AS trip_count,
    AVG(total_amount) AS avg_fare,
    AVG(trip_distance) AS avg_distance
FROM {{ ref('fct_taxi_trips') }}
WHERE DATEDIFF('minute', pickup_datetime, dropoff_datetime) > 0
GROUP BY 1, 2, 3
