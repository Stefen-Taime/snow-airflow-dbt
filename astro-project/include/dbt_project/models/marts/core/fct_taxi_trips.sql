{{
  config(
    materialized='incremental',
    unique_key='trip_surrogate_key',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
  )
}}

SELECT
    t.trip_surrogate_key,
    t.vendor_id,
    v.vendor_name,
    t.pickup_datetime,
    t.dropoff_datetime,
    t.passenger_count,
    t.trip_distance,
    t.pu_location_id,
    pu_zone.zone_name AS pickup_zone,
    pu_zone.borough AS pickup_borough,
    t.do_location_id,
    do_zone.zone_name AS dropoff_zone,
    do_zone.borough AS dropoff_borough,
    t.rate_code_id,
    rc.rate_code_name,
    t.payment_type_id,
    pt.payment_type_name,
    t.fare_amount,
    t.extra,
    t.mta_tax,
    t.tip_amount,
    t.tolls_amount,
    t.improvement_surcharge,
    t.total_amount,
    t.congestion_surcharge,
    t.airport_fee,
    t.cbd_congestion_fee,
    t.taxi_type
FROM {{ ref('int_trips_unioned') }} AS t
LEFT JOIN {{ ref('dim_zones') }} AS pu_zone
    ON t.pu_location_id = pu_zone.location_id
LEFT JOIN {{ ref('dim_zones') }} AS do_zone
    ON t.do_location_id = do_zone.location_id
LEFT JOIN {{ ref('rate_codes') }} AS rc
    ON t.rate_code_id = rc.rate_code_id
LEFT JOIN {{ ref('payment_types') }} AS pt
    ON t.payment_type_id = pt.payment_type_id
LEFT JOIN {{ ref('vendor_lookup') }} AS v
    ON t.vendor_id = v.vendor_id

{% if is_incremental() %}
    WHERE t.pickup_datetime >= (
        SELECT DATEADD('day', -3, MAX(pickup_datetime))
        FROM {{ this }}
    )
{% endif %}
