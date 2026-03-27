{{
  config(
    materialized='table'
  )
}}

SELECT
    location_id,
    borough,
    zone_name,
    service_zone
FROM {{ ref('stg_taxi_zone_lookup') }}
