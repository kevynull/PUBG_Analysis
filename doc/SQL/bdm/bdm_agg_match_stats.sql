create table bdm_agg_match_stats(
    `date` STRING,
    `game_size` INT,
    `match_id` STRING,
    `match_mode` STRING,
    `party_size` INT,
    `player_assists` INT,
    `player_dbno` INT,
    `player_dist_ride` DOUBLE,
    `player_dist_walk` DOUBLE,
    `player_dmg` INT,
    `player_kills` INT,
    `player_name` STRING,
    `player_survive_time` DOUBLE,
    `team_id`  INT,
    `team_placement` INT
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
load data inpath '/data/agg_match_stats_4_top50_demo.csv' into table bdm_agg_match_stats;