{{
  config(
    materialized='table'
  )
}}

SELECT
    LOCATIONID AS LOCATION_ID,
    BOROUGH,
    ZONE AS ZONE_NAME,
    SERVICE_ZONE
FROM {{ source('tlc_reference', 'taxi_zone_lookup') }}
WHERE LOCATIONID NOT IN (264, 265)  -- Filter Unknown zones
