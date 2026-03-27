{{
  config(
    materialized='table'
  )
}}

SELECT
    DATE_TRUNC('month', pickup_datetime)              AS trip_month,
    taxi_type,
    COUNT(*)                                           AS total_trips,
    SUM(total_amount)                                  AS total_revenue,
    AVG(fare_amount)                                   AS avg_fare,
    AVG(trip_distance)                                 AS avg_trip_distance,
    AVG(passenger_count)                               AS avg_passenger_count,
    SUM(cbd_congestion_fee)                            AS total_cbd_fees,
    COUNT(CASE WHEN cbd_congestion_fee > 0 THEN 1 END) AS cbd_trips
FROM {{ ref('fct_taxi_trips') }}
GROUP BY 1, 2
