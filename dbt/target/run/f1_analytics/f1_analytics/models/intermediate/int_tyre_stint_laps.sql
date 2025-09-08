
  create view "f1"."f1_data"."int_tyre_stint_laps__dbt_tmp"
    
    
  as (
    

with cte as (SELECT s.session_key,
                    s.driver_number,
                    s.stint_number,
                    s.compound,
                    s.lap_start,
                    s.lap_end,
                    s.lap_end - s.lap_start + 1 as stint_length,
                    -- Get the start time from the first lap of the stint
                    MIN(l.date_start)           as stint_start_time,
                    -- Get the end time from the last lap of the stint
                    MAX(l.date_start)           as stint_end_time
             FROM "f1"."f1_data"."stg_stints" s
                      LEFT JOIN "f1"."f1_data"."stg_laps" l
                                ON s.session_key = l.session_key
                                    AND s.driver_number = l.driver_number
                                    AND l.lap_number BETWEEN s.lap_start AND s.lap_end
             GROUP BY 1, 2, 3, 4, 5, 6
             )
select * from cte
         where stint_end_time is not null
           and
             stint_start_time is not null
  );