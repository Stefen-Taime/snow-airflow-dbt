{{
  config(
    materialized='table'
  )
}}

SELECT
    rate_code_id,
    rate_code_name
FROM {{ ref('rate_codes') }}
