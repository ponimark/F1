WITH session_drivers AS (
   SELECT DISTINCT
       s.session_key,
       s.country_name,
       s.session_name,
       s.session_type,
       s.year,
       d.driver_number,
       concat(d.name_acronym, ' - ', d.full_name, ' (', d.team_name, ')') as driver_display_name
   FROM "f1"."f1_data"."stg_sessions" s
   JOIN "f1"."f1_data"."stg_drivers" d ON s.session_key = d.session_key
)
SELECT
   s.session_key,
    s.year,
   s.country_name,
   s.circuit_short_name,
   s.session_name,
   s.session_type,
   concat(s.year, ' - ', s.country_name, ' - ', s.session_name) as session_display_name,
   array_agg(DISTINCT sd.driver_display_name) as available_driver_names,
   m.meeting_name,
   m.meeting_official_name,
   s.date_start
FROM "f1"."f1_data"."stg_sessions" s
JOIN session_drivers sd ON s.session_key = sd.session_key
JOIN "f1"."f1_data"."stg_meeting" m ON s.year = m.year
   AND s.country_name = m.country_name
GROUP BY 1,2,3,4,5,6,7,9,10,11