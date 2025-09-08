
  create view "f1"."f1_data"."stg_intervals__dbt_tmp"
    
    
  as (
    


   SELECT
    session_key,
    driver_number,
    date,
    CASE
        WHEN gap_to_leader = 'None' OR gap_to_leader LIKE '%L%' THEN NULL
        ELSE gap_to_leader
    END AS gap_to_leader,
    CASE
        WHEN interval = 'None' OR interval LIKE '%L%' THEN NULL
        ELSE interval
    END AS gap_to_next
FROM "f1"."f1_data"."intervals"
  );