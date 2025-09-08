{{ config(materialized='view') }}

    WITH source AS (
    SELECT * FROM {{ source('raw', 'position') }}
)
SELECT
    session_key,
    driver_number,
    position,
    date
FROM source