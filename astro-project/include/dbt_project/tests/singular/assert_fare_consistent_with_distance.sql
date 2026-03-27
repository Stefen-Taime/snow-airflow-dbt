-- Singular test: trips > 5 miles with $0 fare on standard rate are suspicious
-- config: severity = warn (real TLC data has some anomalous records)
{{ config(severity='warn') }}

SELECT *
FROM {{ ref('fct_taxi_trips') }}
WHERE trip_distance > 5
  AND fare_amount = 0
  AND rate_code_id = 1
