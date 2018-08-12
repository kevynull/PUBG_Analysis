create table gdm_player_career_stats_model(
    `name` STRING,                             --玩家姓名
    `first_play_time` STRING,                  --玩家第一次游戏的时间
    `last_play_time`STRING,                   --玩家最近一次游戏的时间
    `total_kills` INT,                      --玩家总击杀数
    `total_assists` INT,                    --玩家总助攻数
    `total_death` INT,                      --玩家总死亡数
    `total_suvive_time` INT,                --玩家总生成时间（秒）
    `total_dmg` INT,                        --玩家总伤害值
    `play_count` INT,                       --玩家游戏总场数
    `total_dbno` INT,                       --玩家倒下但未阵亡总次数
    `total_dist_ride` DOUBLE,                  --玩家使用载具总距离
    `total_dist_walk` DOUBLE,                  --玩家步行总距离
    `online_stages_start` INT,              --习惯在线时段开始
    `online_stages_end` INT                --习惯在线时段结束
)
partitioned by (`first_play_date` STRING)



--用户表的分区，是第一次出现击杀记录那天，【first_play_time】的数据处理
