

SELECT
   ts.driver_number,
   ts.session_key,
   d.name_acronym,
   d.team_name,
   d.team_colour,
   ts.date,
   ts.rpm,
   ts.speed,
   ts.throttle,
   ts.brake
FROM "f1"."f1_data"."stg_car_data" ts
LEFT JOIN "f1"."f1_data"."stg_drivers" d
   ON ts.driver_number = d.driver_number
   AND ts.session_key = d.session_key