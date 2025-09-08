
  create view "f1"."f1_data"."final_race_summary__dbt_tmp"
    
    
  as (
    

WITH positions AS (
   SELECT
       session_key,
       COUNT(DISTINCT driver_number) as total_drivers

   FROM "f1"."f1_data"."stg_position"
   GROUP BY session_key
),
laps AS (
   SELECT
       session_key,
       MAX(lap_number) as total_laps
   FROM "f1"."f1_data"."stg_laps"
   GROUP BY session_key
),
team_stats AS (
   SELECT
       session_key,
       SUM(pit_stop_count) as total_pit_stops,
        COUNT(DISTINCT team_name) as total_teams
   FROM "f1"."f1_data"."final_race_leaderboard"
   GROUP BY session_key
),
driver_stats AS (
   SELECT
       session_key,
       driver_number,
       MAX(speed) as top_speed,
       AVG(speed) as avg_speed
   FROM "f1"."f1_data"."stg_car_data"
   GROUP BY session_key, driver_number
),
cte as(
SELECT
   p.session_key,
   m.session_display_name,
   p.total_drivers,
   l.total_laps,
   MAX(d.top_speed) as fastest_speed,
   round(AVG(d.avg_speed),3) as average_speed
FROM positions p
JOIN laps l ON p.session_key = l.session_key
JOIN "f1"."f1_data"."final_session_mapping" m ON p.session_key = m.session_key
LEFT JOIN driver_stats d ON p.session_key = d.session_key
GROUP BY p.session_key, m.session_display_name, p.total_drivers, l.total_laps
)
select c.session_key,
   c.session_display_name,
   c.total_drivers,
    t.total_teams,
   c.total_laps,
   t.total_pit_stops,
    c.fastest_speed,
   c.average_speed
from cte c left join team_stats t on c.session_key=t.session_key
  );