{{
  config(
    materialized='table'
  )
}}

SELECT
    DATE_TRUNC('day', pickup_datetime) AS trip_date,
    taxi_type,
    COUNT(*) AS total_trips,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS avg_revenue_per_trip,
    SUM(total_amount) / NULLIF(SUM(trip_distance), 0) AS revenue_per_mile,
    SUM(fare_amount) AS total_fares,
    SUM(tip_amount) AS total_tips,
    AVG(tip_amount) / NULLIF(AVG(fare_amount), 0) * 100 AS avg_tip_pct,
    SUM(tolls_amount) AS total_tolls,
    SUM(congestion_surcharge) AS total_congestion_surcharges,
    SUM(airport_fee) AS total_airport_fees,
    SUM(cbd_congestion_fee) AS total_cbd_fees
FROM {{ ref('fct_taxi_trips') }}
GROUP BY 1, 2
