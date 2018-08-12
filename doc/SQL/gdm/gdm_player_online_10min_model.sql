create table gdm_player_online_10min_model(
`player_count` INT,                     --玩家数量
`player_game_time` INT,                 --玩家游戏时间（秒）
`time` STRING,                             --时间（例如：12:30:00）
`hour` INT,                             --小时（例如：12）
`minute` INT                           --分钟（例如：30）
)
partitioned by (`date` STRING)