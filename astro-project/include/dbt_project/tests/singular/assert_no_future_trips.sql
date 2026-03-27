-- Singular test: no trips should have a pickup datetime in the future
SELECT *
FROM {{ ref('fct_taxi_trips') }}
WHERE pickup_datetime > CURRENT_TIMESTAMP()
