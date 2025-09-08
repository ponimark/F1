{{ config(materialized='view') }}

SELECT
   t.date,
   CONCAT(t.name_acronym, ' (', t.team_name, ')') as driver,
   t.driver_number,
   ROUND(t.speed) as speed,
   ROUND(t.throttle) as throttle,
   ROUND(t.brake) as brake,
   t.rpm,
   t.session_key,  -- Add session_key
   m.meeting_key   -- Add meeting_key
FROM {{ ref('int_car_base') }} t
LEFT JOIN {{ ref('stg_sessions') }} s ON t.session_key = s.session_key
LEFT JOIN {{ ref('stg_meeting') }} m ON s.meeting_key = m.meeting_key