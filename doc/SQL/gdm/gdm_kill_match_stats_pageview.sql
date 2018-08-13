create table gdm_kill_match_stats_pageview(
`pubg_opgg_id` STRING,                     --pubg战绩pubg.op.gg查询ID
`times` INT,                             --击杀时间（开局到击杀发生的秒数）
`killed_by` STRING,                        --击杀方式
`killer_name` STRING,                      --击杀人名字
`killer_placement` INT,                 --击杀人位置（玩家列表位置）
`killer_position_x` DOUBLE,                --击杀位置X坐标
`killer_position_y` DOUBLE,                --击杀位置Y坐标
`killer_position_600_x` DOUBLE,            --击杀位置X坐标（比例缩放600）
`killer_position_600_y` DOUBLE,            --击杀位置Y坐标（比例缩放600）
`killer_position_800_x` DOUBLE,            --击杀位置X坐标（比例缩放800）
`killer_position_800_y` DOUBLE,            --击杀位置Y坐标（比例缩放800）
`map` STRING,                              --击杀出现的地图
`victim_name` STRING,                      --受害者名称
`victim_placement` INT,                 --受害者位置（玩家列表位置）
`victim_position_x` DOUBLE,                --受害者位置X坐标
`victim_position_y` DOUBLE,                --受害者位置Y坐标
`victim_position_600_x` DOUBLE,            --受害者位置X坐标（比例缩放600）
`victim_position_600_y` DOUBLE,            --受害者位置Y坐标（比例缩放600）
`victim_position_800_x` DOUBLE,            --受害者位置X坐标（比例缩放800）
`victim_position_800_y` DOUBLE,            --受害者位置Y坐标（比例缩放800）
`shot_distance` DOUBLE                    --射击距离（m）
)
partitioned by (`date` STRING)