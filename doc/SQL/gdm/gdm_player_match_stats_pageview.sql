create table gdm_player_match_stats_pageview(
`time` STRING,                             --[date]处理字段，格式：hh-MM-ss
`pubg_opgg_id` STRING,                     --pubg战绩pubg.op.gg查询ID
`match_mode` STRING,                       --比赛模式
`maximum_distance_shot` DOUBLE,            --最远距离射击
`player_assists` INT,                   --玩家助攻
`player_dbno` INT,                      --玩家倒下但未阵亡
`player_dist_ride` DOUBLE,                 --玩家使用载具距离
`player_dist_walk` DOUBLE,                 --玩家步行距离
`player_dmg` INT,                       --玩家伤害值（damage）
`player_kills` INT,                     --玩家击杀数
`player_name` STRING,                      --玩家名字
`player_suvive_time` DOUBLE               --玩家生存时间（秒）
`is_party_team` INT,                    --是否组队
`party_size` INT,                       --组队人数
`is_winner` INT,                        --是否胜利
`is_use_ride` INT,                      --是否使用载具
`team_placement` INT
)
partitioned by (`date` STRING)