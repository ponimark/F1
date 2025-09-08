{{ config(materialized='view') }}

SELECT DISTINCT
    EXTRACT(YEAR FROM date_start)::text || ' - ' || country_name || ' - ' || session_name AS session_display_name,
    session_key
FROM {{ ref('stg_sessions') }}
