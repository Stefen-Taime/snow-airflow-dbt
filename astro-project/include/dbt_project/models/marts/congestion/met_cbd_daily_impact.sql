{{
  config(
    materialized='table'
  )
}}

SELECT
    DATE_TRUNC('day', pickup_datetime) AS trip_date,
    taxi_type,
    COUNT(*) AS total_trips,
    COUNT(CASE WHEN cbd_congestion_fee > 0 THEN 1 END) AS cbd_trips,
    ROUND(
        COUNT(CASE WHEN cbd_congestion_fee > 0 THEN 1 END) * 100.0
        / NULLIF(COUNT(*), 0), 2
    ) AS cbd_trip_pct,
    SUM(cbd_congestion_fee) AS total_cbd_fees,
    AVG(CASE
        WHEN cbd_congestion_fee > 0
            THEN cbd_congestion_fee
    END) AS avg_cbd_fee,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS avg_revenue_per_trip,
    SUM(total_amount) / NULLIF(SUM(trip_distance), 0) AS revenue_per_mile,
    AVG(trip_distance / NULLIF(
        DATEDIFF('minute', pickup_datetime, dropoff_datetime) / 60.0, 0
    )) AS avg_speed_mph
FROM {{ ref('fct_taxi_trips') }}
GROUP BY 1, 2
