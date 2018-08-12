create table gdm_match_stats_model(
`time` STRING,                             --比赛开始时间；[date]处理字段，格式：hh-MM-ss
`match_time` INT,                       --比赛进行时间（秒数）
`map` STRING,                              --比赛使用的地图
`match_size` INT,                       --比赛玩家人数
`match_mode` STRING,                       --比赛模式
`player_kills` INT,                     --玩家总击杀数（此局）
`player_deaths` INT,                    --玩家总死亡数（此局）
`player_rides` INT,                     --使用过载具的玩家数量
`match_dist_ride` DOUBLE,                  --比赛中所有玩家使用载具总距离
`match_dist_walk` DOUBLE,                  --比赛中所有玩家步行总距离
`match_dmg` INT,                        --比赛中玩家照成的总伤害
`maximum_distance_shot` DOUBLE,            --最远距离射击
`md_shot_name` STRING,                     --最远距离射击的玩家名称
`md_shot_player_model` STRING             --最远距离射击的玩家模型ID
)
partitioned by (`date` STRING)