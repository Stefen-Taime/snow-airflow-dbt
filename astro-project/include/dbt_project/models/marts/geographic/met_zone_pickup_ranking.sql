{{
  config(
    materialized='table'
  )
}}

SELECT
    DATE_TRUNC('month', pickup_datetime) AS trip_month,
    pickup_borough,
    pickup_zone,
    pu_location_id,
    COUNT(*)                              AS trip_count,
    SUM(total_amount)                     AS total_revenue,
    AVG(total_amount)                     AS avg_revenue_per_trip
FROM {{ ref('fct_taxi_trips') }}
GROUP BY 1, 2, 3, 4
