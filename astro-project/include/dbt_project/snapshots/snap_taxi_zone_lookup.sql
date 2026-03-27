{% snapshot snap_taxi_zone_lookup %}

{{
  config(
    target_schema='snapshots',
    unique_key='LocationID',
    strategy='check',
    check_cols=['Borough', 'Zone', 'service_zone'],
    dbt_valid_to_current="'9999-12-31'::timestamp_ntz",
    invalidate_hard_deletes=True
  )
}}

SELECT * FROM {{ source('tlc_reference', 'taxi_zone_lookup') }}

{% endsnapshot %}
