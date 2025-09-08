

    select
        driver_number,
        session_key,
        max(cast(gap_to_leader as float)) as max_gap_to_leader,
        max(cast(interval as float)) as max_gap_to_next,
        from F1.PONI.stg_intervals
        group by 1,2