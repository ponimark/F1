{{ config(materialized='view') }}


select *
FROM {{ source('raw','weather') }}
