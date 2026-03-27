{{
  config(
    materialized='table'
  )
}}

SELECT
    DATE_TRUNC('month', pickup_datetime) AS trip_month,
    taxi_type,
    CASE
        WHEN
            DAYOFWEEK(pickup_datetime) IN (0, 6) -- weekend
            AND HOUR(pickup_datetime) BETWEEN 9 AND 20
            THEN 'Peak (Weekend 9am-9pm)'
        WHEN
            DAYOFWEEK(pickup_datetime) NOT IN (0, 6) -- weekday
            AND HOUR(pickup_datetime) BETWEEN 5 AND 20
            THEN 'Peak (Weekday 5am-9pm)'
        ELSE 'Off-Peak'
    END AS time_category,
    COUNT(CASE WHEN cbd_congestion_fee > 0 THEN 1 END) AS cbd_trips,
    SUM(cbd_congestion_fee) AS total_cbd_fees,
    AVG(CASE
        WHEN cbd_congestion_fee > 0
            THEN cbd_congestion_fee
    END) AS avg_cbd_fee
FROM {{ ref('fct_taxi_trips') }}
GROUP BY 1, 2, 3
