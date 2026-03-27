{{
  config(
    materialized='incremental',
    unique_key='trip_surrogate_key',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
  )
}}

WITH source AS (
    SELECT
        vendorid AS vendor_id,
        tpep_pickup_datetime AS pickup_datetime,
        tpep_dropoff_datetime AS dropoff_datetime,
        passenger_count,
        trip_distance,
        pulocationid AS pu_location_id,
        dolocationid AS do_location_id,
        ratecodeid AS rate_code_id,
        payment_type AS payment_type_id,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge,
        airport_fee AS airport_fee,
        cbd_congestion_fee,
        'yellow' AS taxi_type,
        load_timestamp
    FROM {{ source('tlc_trips', 'yellow_taxi_trips') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY
            vendor_id, pickup_datetime, dropoff_datetime,
            pu_location_id, do_location_id, fare_amount, trip_distance
        ORDER BY load_timestamp DESC
    ) = 1
)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'vendor_id', 'pickup_datetime', 'dropoff_datetime',
        'pu_location_id', 'do_location_id', 'fare_amount',
        'trip_distance', 'payment_type_id'
    ]) }} AS trip_surrogate_key,
    vendor_id,
    pickup_datetime,
    dropoff_datetime,
    passenger_count,
    trip_distance,
    pu_location_id,
    do_location_id,
    rate_code_id,
    payment_type_id,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee,
    cbd_congestion_fee,
    taxi_type,
    load_timestamp
FROM source

{% if is_incremental() %}
    WHERE pickup_datetime >= (
        SELECT DATEADD('day', -3, MAX(pickup_datetime))
        FROM {{ this }}
    )
{% endif %}
