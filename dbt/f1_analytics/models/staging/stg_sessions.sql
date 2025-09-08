{{ config(materialized='view') }}

{#with raw as (#}
{#    select * from {{ source('raw','sessions') }}#}
{#),#}
{#raw1 as (#}
{#    select * from {{ source('raw','meetings') }}#}
{#)#}
{#select#}
{#  cast(s.session_key as int)           as session_key,#}
{#  cast(s.meeting_key as int)           as meeting_key,#}
{#  s.session_type                       as session_type,#}
{#  m.circuit_short_name,#}
{#  m.country_name,#}
{#  s.date_start::timestamp              as date_start,#}
{#  s.date_end::timestamp                as date_end,#}
{#  extract(year from s.date_start)      as year#}
{#from raw s#}
{#left join raw1 m on s.meeting_key = m.meeting_key#}
{#where session_key is not null#}
{#  and session_type in ('Race', 'Qualifying')#}

WITH source AS (
    SELECT
        session_key,
        meeting_key,
        country_name,
        session_name,
        session_type,
        year,
        circuit_short_name,
        date_start,
        date_end
    FROM {{ source('raw', 'sessions') }}
)
SELECT * FROM source
