{{
  config(
    materialized='table'
  )
}}

SELECT
    DATE_TRUNC('day', pickup_datetime)   AS trip_date,
    taxi_type,
    COUNT(*)                              AS trip_count,
    AVG(trip_distance)                    AS avg_distance,
    AVG(DATEDIFF('minute', pickup_datetime, dropoff_datetime)) AS avg_duration_min,
    AVG(trip_distance / NULLIF(
        DATEDIFF('minute', pickup_datetime, dropoff_datetime) / 60.0, 0
    ))                                    AS avg_speed_mph
FROM {{ ref('fct_taxi_trips') }}
WHERE DATEDIFF('minute', pickup_datetime, dropoff_datetime) > 0
  AND trip_distance > 0
GROUP BY 1, 2
