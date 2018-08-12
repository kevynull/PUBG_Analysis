create table bdm_kill_match_stats(
    `killed_by` STRING,
    `killer_name` STRING,
    `killer_placement` DOUBLE,
    `killer_position_x` DOUBLE,
    `killer_position_y` DOUBLE,
    `map` STRING,
    `match_id` STRING,
    `time` INT,
    `victim_name` STRING,
    `victim_placement` DOUBLE,
    `victim_position_x` DOUBLE,
    `victim_position_y` DOUBLE
) 
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
with serdeproperties(
    "separatorChar"=",",
    "quoteChar"="'",
    "escapeChar"= "\\"
) stored as textfile 
tblproperties ("skip.header.line.count"="1");
-- skip.header.line.count 跳过头行

--row format delimited fields terminated by ','
--location '/business/bdm/itcast_bdm_user

--------------
load data inpath '/data/kill_match_stats_final_4_top50_demo.csv' into table bdm_deaths;