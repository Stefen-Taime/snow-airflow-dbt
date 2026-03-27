{{
  config(
    materialized='table'
  )
}}

SELECT
    vendor_id,
    vendor_name
FROM {{ ref('vendor_lookup') }}
