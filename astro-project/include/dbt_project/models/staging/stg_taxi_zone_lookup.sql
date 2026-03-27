{{
  config(
    materialized='table'
  )
}}

SELECT
    LocationID   AS location_id,
    Borough      AS borough,
    Zone         AS zone_name,
    service_zone
FROM {{ source('tlc_reference', 'taxi_zone_lookup') }}
WHERE LocationID NOT IN (264, 265)  -- Filter Unknown zones
