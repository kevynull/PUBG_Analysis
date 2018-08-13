create table fdm_agg_match_wide(
    `time` STRING,                             --[date]处理字段，格式：hh-MM-ss
    `year` INT,                             --[date]处理字段，年
    `month` INT,                            --[date]处理字段，月
    `day` INT,                              --[date]处理字段，日
    `hour` INT,                             --[date]处理字段，时
    `minute` INT,                           --[date]处理字段，分
    `seconds` INT,                          --[date]处理字段，秒
    `game_size` INT,                        --进场游戏人数
    `match_id` STRING,                         --匹配ID
    `match_mode` STRING,                       --匹配模式
    `party_size` INT,                       --组队人数（是单排，还是开黑）
    `player_assists` INT,                   --玩家助攻
    `player_dbno` INT,                      --玩家倒下但未阵亡
    `player_dist_ride` DOUBLE,                 --玩家使用载具距离
    `player_dist_walk` DOUBLE,                 --玩家步行距离
    `player_dmg` INT,                       --玩家伤害值（damage）
    `player_kills` INT,                     --玩家击杀数
    `player_name` STRING,                      --玩家名字
    `player_suvive_time` DOUBLE,               --玩家生存时间（秒）
    `team_id` INT,                          --组队ID
    `team_placement` INT,                    --组队位置（玩家列表位置）
    `is_use_ride` INT
)
partitioned by (`date` STRING)                             --分区字段，格式：yyyy-mm-dd
