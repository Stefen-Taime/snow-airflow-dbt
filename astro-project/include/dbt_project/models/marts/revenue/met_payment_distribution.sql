{{
  config(
    materialized='table'
  )
}}

SELECT
    DATE_TRUNC('month', pickup_datetime) AS trip_month,
    taxi_type,
    payment_type_name,
    COUNT(*)                              AS trip_count,
    SUM(total_amount)                     AS total_revenue,
    AVG(tip_amount)                       AS avg_tip,
    AVG(tip_amount) / NULLIF(AVG(fare_amount), 0) * 100 AS avg_tip_pct
FROM {{ ref('fct_taxi_trips') }}
GROUP BY 1, 2, 3
