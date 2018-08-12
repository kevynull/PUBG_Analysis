create table fdm_kill_match_wide(
    `killed_by` STRING,
    `killer_name` STRING,
    `killer_placement` INT,
    `killer_position_x` DOUBLE,
    `killer_position_y` DOUBLE,
    `map` STRING,
    `match_id` STRING,
    `times` INT,
    `victim_name` STRING,
    `victim_placement` INT,
    `victim_position_x` DOUBLE,
    `victim_position_y` DOUBLE
)
partitioned by (`date` STRING)                             --分区字段，格式：yyyy-mm-dd