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
    dropoff_borough,
    dropoff_zone,
    COUNT(*)                              AS trip_count,
    AVG(total_amount)                     AS avg_fare,
    AVG(trip_distance)                    AS avg_distance,
    AVG(DATEDIFF('minute', pickup_datetime, dropoff_datetime)) AS avg_duration_min
FROM {{ ref('fct_taxi_trips') }}
GROUP BY 1, 2, 3, 4, 5, 6
