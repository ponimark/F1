{{ config(materialized='view') }}

WITH final_rankings AS (
   SELECT
       session_key,
       driver_number,
       position
   FROM int_final_position
)
SELECT
   fr.session_key,
   d.driver_number,
   d.broadcast_name AS driver_name,
   fr.position,
   d.team_name,
   d.team_colour AS team_color,
   CASE
       WHEN fr.position = 1 THEN 0
       ELSE fi.max_gap_to_leader
   END as gap_to_leader,
   fi.max_gap_to_next as gap_to_next,
   bl.fastest_lap,
   ps.pit_stop_count,

   CASE
       WHEN fr.position = 1 THEN 25
       WHEN fr.position = 2 THEN 18
       WHEN fr.position = 3 THEN 15
       WHEN fr.position = 4 THEN 12
       WHEN fr.position = 5 THEN 10
       WHEN fr.position = 6 THEN 8
       WHEN fr.position = 7 THEN 6
       WHEN fr.position = 8 THEN 4
       WHEN fr.position = 9 THEN 2
       WHEN fr.position = 10 THEN 1
       ELSE 0
   END AS points
FROM final_rankings fr
JOIN {{ ref('stg_drivers') }} d
   ON fr.driver_number = d.driver_number
   AND fr.session_key = d.session_key
LEFT JOIN {{ ref('int_final_intervals') }} fi
   ON fr.session_key = fi.session_key
   AND fr.driver_number = fi.driver_number
LEFT JOIN {{ ref('int_best_laps') }} bl
   ON fr.session_key = bl.session_key
   AND fr.driver_number = bl.driver_number
LEFT JOIN {{ ref('int_pit_stop') }} ps
   ON fr.session_key = ps.session_key
   AND fr.driver_number = ps.driver_number
JOIN {{ ref('final_session_mapping') }} m
   ON fr.session_key = m.session_key
ORDER BY fr.position