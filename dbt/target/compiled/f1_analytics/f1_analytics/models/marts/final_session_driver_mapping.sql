

SELECT DISTINCT
    d.session_key,
    concat(d.name_acronym, ' - ', d.full_name, ' (', d.team_name, ')') as driver_display_name,
    d.driver_number
FROM "f1"."f1_data"."stg_drivers" d
JOIN "f1"."f1_data"."stg_sessions" s ON d.session_key = s.session_key