{{
  config(
    materialized='table'
  )
}}

SELECT
    DATE_TRUNC('month', pickup_datetime) AS trip_month,
    taxi_type,
    CASE
        WHEN pu_location_id IN (132, 138) THEN pickup_zone
        WHEN do_location_id IN (132, 138) THEN dropoff_zone
    END                                   AS airport_name,
    CASE
        WHEN pu_location_id IN (132, 138) THEN 'pickup'
        WHEN do_location_id IN (132, 138) THEN 'dropoff'
    END                                   AS trip_direction,
    COUNT(*)                              AS trip_count,
    SUM(total_amount)                     AS total_revenue,
    AVG(total_amount)                     AS avg_fare,
    AVG(trip_distance)                    AS avg_distance,
    AVG(tip_amount) / NULLIF(AVG(fare_amount), 0) * 100 AS avg_tip_pct
FROM {{ ref('fct_taxi_trips') }}
WHERE pu_location_id IN (132, 138) OR do_location_id IN (132, 138)
GROUP BY 1, 2, 3, 4
