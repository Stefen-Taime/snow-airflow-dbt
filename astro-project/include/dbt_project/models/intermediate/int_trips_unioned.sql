{{
  config(
    materialized='ephemeral'
  )
}}

SELECT * FROM {{ ref('stg_yellow_taxi_trips') }}
UNION ALL
SELECT * FROM {{ ref('stg_green_taxi_trips') }}
