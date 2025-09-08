{{ config(materialized='view') }}

SELECT
    session_key,
    driver_number,
    date,
    rpm,
    speed,
    throttle,
    brake
FROM {{ source('raw', 'car_data') }}