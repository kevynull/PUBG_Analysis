create table gdm_player_career_stats_model (
    `name` STRING,                             --玩家姓名
    `first_play_time` STRING,                  --玩家第一次游戏的时间
    `last_play_time` STRING,                   --玩家最近一次游戏的时间
    `total_kills` INT,   
    `avg_kills` DOUBLE,                   --玩家总击杀数
    `total_assists` INT,                    --玩家总助攻数
    `avg_assists` DOUBLE,
    `total_suvive_time` DOUBLE,                --玩家总生存时间（秒）
    `avg_suvive_time` DOUBLE,  
    `total_dmg` INT,                        --玩家总伤害值
    `avg_dmg` DOUBLE,
    `play_count` INT,                       --玩家游戏总场数
    `win_count` INT,                        --玩家赢的游戏总场数
    `party_count` INT,                      --玩家组队游戏总场数
    `total_dbno` INT,                       --玩家倒下但未阵亡总次数
    `total_dist_ride` DOUBLE,                  --玩家使用载具总距离
    `max_dist_ride` DOUBLE,
    `max_dist_ride_match` STRING,
    `total_dist_walk` DOUBLE,                  --玩家步行总距离
    `max_dist_walk` DOUBLE,
    `max_dist_walk_match` STRING,
    `online_stages` INT,              --习惯在线时段
    `max_dist_shot` DOUBLE,
    `max_dist_shot_match` STRING,
    `max_suvive_time` DOUBLE,                   --最大生存时间
    `max_suvive_time_match` STRING,            --最大生存时间比赛ID
    `max_kills` INT,                        --最大击杀次数
    `max_kills_match` STRING,                  --最大击杀次数比赛ID
    `max_assists` INT,                      --最大助攻次数
    `max_assists_match` STRING,                --最大助攻次数比赛ID
    `count_use_ride` INT,                   --载具使用次数
    `kill_death_ratio` DOUBLE,                  --KDA
    `top_10_ratio` DOUBLE
)
partitioned by (`first_play_date` STRING)



--用户表的分区，是第一次出现击杀记录那天，【first_play_time】的数据处理
