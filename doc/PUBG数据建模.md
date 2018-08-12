# PUBG数据建模

## BDM：缓存数据层，元数据的直接映像

​    将hdfs中的csv数据集直接映射到hive中。

### bdm_agg_match_stats：玩家比赛信息统计表

```SQL
date,                             --日期
game_size,                        --进场游戏人数
match_id,                         --匹配ID（pubg战绩pubg.op.gg查询ID）
match_mode,                       --匹配模式
party_size,                       --组队人数（是单排，还是开黑）
player_assists,                   --玩家助攻
player_dbno,                      --玩家倒下但未阵亡
player_dist_ride,                 --玩家使用载具距离
player_dist_walk,                 --玩家步行距离
player_dmg,                       --玩家伤害值（damage）
player_kills,                     --玩家击杀数
player_name,                      --玩家名字
player_suvive_time,               --玩家生存时间（秒）
team_id,                          --组队ID
team_placement                    --组队位置（玩家列表位置）
```



### bdm_kill_match_stats：比赛击杀信息明细表

```SQL
killed_by,                        --击杀方式
killer_name,                      --击杀人名字
killer_placement,                 --击杀人位置（玩家列表位置）
killer_position_x,                --击杀位置X坐标
killer_position_y,                --击杀位置Y坐标
map,                              --击杀出现的地图
match_id,                         --匹配ID（pubg战绩pubg.op.gg查询ID）
time,                             --击杀时间（开局到击杀发生的秒数）
victim_name,                      --受害者名称
victim_placement,                 --受害者位置（玩家列表位置）
victim_position_x,                --受害者位置X坐标
victim_position_y                 --受害者位置Y坐标

```



## FDM：基础数据层，数据拉链处理，分区处理

​    将bdm层的数据进行简单的增宽处理，同时按天进行分区。

### fdm_agg_match_wide：玩家比赛信息统计宽表

```SQL
date,                             --分区字段，格式：yyyy-mm-dd
time,                             --[date]处理字段，格式：hh-MM-ss
year,                             --[date]处理字段，年
month,                            --[date]处理字段，月
day,                              --[date]处理字段，日
hour,                             --[date]处理字段，时
minute,                           --[date]处理字段，分
seconds,                          --[date]处理字段，秒
game_size,                        --进场游戏人数
match_id,                         --匹配ID（pubg战绩pubg.op.gg查询ID）
match_mode,                       --匹配模式
party_size,                       --组队人数（是单排，还是开黑）
player_assists,                   --玩家助攻
player_dbno,                      --玩家倒下但未阵亡
player_dist_ride,                 --玩家使用载具距离
player_dist_walk,                 --玩家步行距离
player_dmg,                       --玩家伤害值（damage）
player_kills,                     --玩家击杀数
player_name,                      --玩家名字
player_suvive_time,               --玩家生存时间（秒）
team_id,                          --组队ID
team_placement                    --组队位置（玩家列表位置）
```



### fdm_kill_match_wide：比赛击杀信息明细宽表

```SQL
date,                             --分区字段，格式：yyyy-mm-dd
killed_by,                        --击杀方式
killer_name,                      --击杀人名字
killer_placement,                 --击杀人位置（玩家列表位置）
killer_position_x,                --击杀位置X坐标
killer_position_y,                --击杀位置Y坐标
map,                              --击杀出现的地图
match_id,                         --匹配ID（pubg战绩pubg.op.gg查询ID）
times,                            --击杀时间（开局到击杀发生的秒数）
victim_name,                      --受害者名称
victim_placement,                 --受害者位置（玩家列表位置）
victim_position_x,                --受害者位置X坐标
victim_position_y                 --受害者位置Y坐标
```

### fdm_kill_weapon_stats：击杀武器信息统计表

```SQL
match_id,                         --匹配ID（pubg战绩pubg.op.gg查询ID）
name,                             --武器名称
shot_distance,                    --武器击杀距离
map,                              --武器出现地图
```

<!-- 
  每一层的数据都是独立的，与其他层无关，如果需要用到其他层的数据，就需要在
  建表的时候，将数据清洗过来。
-->
## GDM：通用聚合层，数据维度处理

​    数据建模，在此层更具需求主题，构建数据仓库模型。

### gdm_player_career_stats_model：玩家生涯统计模型表
<!-- 
  统计玩家生涯数据，即所有玩家相关信息的整合。
-->
```SQL
name,                             --玩家姓名
first_play_time,                  --玩家第一次游戏的时间
first_play_date,                  --分区字段，[first_play_time]格式：yyyy-mm-dd
last_play_time,                   --玩家最近一次游戏的时间
total_kills,                      --玩家总击杀数
total_assists,                    --玩家总助攻数
total_death,                      --玩家总死亡数
total_suvive_time,                --玩家总生成时间（秒）
total_dmg,                        --玩家总伤害值
play_count,                       --玩家游戏总场数
total_dbno,                       --玩家倒下但未阵亡总次数
total_dist_ride,                  --玩家使用载具总距离
total_dist_walk,                  --玩家步行总距离
online_stages_start,              --习惯在线时段开始
online_stages_end,                --习惯在线时段结束
```

### gdm_match_stats_model：比赛统计模型表
<!-- 
  每一局比赛的统计数据
-->
```SQL
date,                             --比赛开始日期；分区字段，格式：yyyy-mm-dd
time,                             --比赛开始时间；[date]处理字段，格式：hh-MM-ss
match_time,                       --比赛进行时间（秒数）
pubg_opgg_id,                     --pubg战绩pubg.op.gg查询ID
map,                              --比赛使用的地图
match_size,                       --比赛玩家人数
match_mode,                       --比赛模式
player_kills,                     --玩家总击杀数（此局）
player_deaths,                    --玩家总死亡数（此局）
player_rides,                     --使用过载具的玩家数量
match_dist_ride,                  --比赛中所有玩家使用载具总距离
match_dist_walk,                  --比赛中所有玩家步行总距离
match_dmg,                        --比赛中玩家照成的总伤害
maximum_distance_shot,            --最远距离射击
md_shot_name,                     --最远距离射击的玩家名称
md_shot_player_model,             --最远距离射击的玩家模型ID
```

### gdm_player_match_stats_pageview：玩家比赛统计视图表
<!-- 
  每一局比赛的每位玩家局内数据（相当于2张基础表的混合统计）
-->
```SQL
date,                             --分区字段，格式：yyyy-mm-dd
time,                             --[date]处理字段，格式：hh-MM-ss
pubg_opgg_id,                     --pubg战绩pubg.op.gg查询ID
match_mode,                       --比赛模式
maximum_distance_shot,            --最远距离射击(cm)
player_assists,                   --玩家助攻
player_dbno,                      --玩家倒下但未阵亡
player_dist_ride,                 --玩家使用载具距离
player_dist_walk,                 --玩家步行距离
player_dmg,                       --玩家伤害值（damage）
player_kills,                     --玩家击杀数
player_name,                      --玩家名字
player_suvive_time,               --玩家生存时间（秒）
```

### gdm_kill_match_stats_pageview：比赛击杀信息明细视图表
<!-- 
  每一局比赛的玩家击杀数据明细
-->
```SQL
date,                             --分区字段，格式：yyyy-mm-dd
match_id,                         --匹配ID（gdm_match_stats_model）
pubg_opgg_id,                     --pubg战绩pubg.op.gg查询ID
times,                             --击杀时间（开局到击杀发生的秒数）
killed_by,                        --击杀方式
killer_name,                      --击杀人名字
killer_id,                        --击杀人id(gdm_player_career_stats_model)
killer_placement,                 --击杀人位置（玩家列表位置）
killer_position_x,                --击杀位置X坐标
killer_position_y,                --击杀位置Y坐标
killer_position_600_x,            --击杀位置X坐标（比例缩放600）
killer_position_600_y,            --击杀位置Y坐标（比例缩放600）
killer_position_800_x,            --击杀位置X坐标（比例缩放800）
killer_position_800_y,            --击杀位置Y坐标（比例缩放800）
map,                              --击杀出现的地图
victim_name,                      --受害者名称
victim_id,                        --受害者id(gdm_player_career_stats_model)
victim_placement,                 --受害者位置（玩家列表位置）
victim_position_x,                --受害者位置X坐标
victim_position_y,                --受害者位置Y坐标
victim_position_600_x,            --受害者位置X坐标（比例缩放600）
victim_position_600_y,            --受害者位置Y坐标（比例缩放600）
victim_position_800_x,            --受害者位置X坐标（比例缩放800）
victim_position_800_y,            --受害者位置Y坐标（比例缩放800）
shot_distance,                    --射击距离（m）
```


### gdm_player_online_10min_model：玩家在线(10分钟)/模型
<!-- 
  统计用户在线情况，10分钟一次。
-->
```SQL
player_count,                     --玩家数量
player_game_time,                 --玩家游戏时间（秒）
date,                             --统计日期，分区字段，格式：yyyy-mm-dd
time,                             --时间（例如：12:30:00）
hour,                             --小时（例如：12）
minute,                           --分钟（例如：30）
```

### gdm_wepons_model：武器模型
<!-- 
  定义武器模型（固定ID），基于（gdm_kill_match_stats_pageview）
-->
```SQL
name,                             --武器名称
avg_shot_distance,                --武器平均击杀距离
max_shot_distance,                --武器最远击杀距离
map                               --武器出现地图
```

## ADM：高度聚合层，提取维度内容进行汇总查询，面向应用
<!-- 
  两坐标之间的距离为：√[(x1-x2)²+(y1-y2)²]
-->

​    面向应用，构建可以简易查询的数据表。此层使用mysql进行数据管理。

<!-- duration.html start 时长占比-->
### adm_player_count_stages：在一个时间段内玩家数量/游戏时间
<!-- 
  统计在一个时间段内的玩家人数，以及平均在线时长（每个玩家完了多久）。
  例如：总游戏时长为16小时，有2人在线，那么，平均在线时长为 8小时/人
  时间区域，1小时统计一次。
  https://blog.csdn.net/duqi_yc/article/details/8292050
-->
```SQL
id,                               --自定义ID字段
date,                             --日期，格式：yyyy-mm-dd
time,                             --时间，格式：hh-MM-ss
player_count,                     --玩家数量
game_time,                        --游戏时间（秒）
```

### adm_player_online_time：玩家游戏时长
<!-- 
  所有玩家的游戏时长。
  例如：玩家A，游戏时间3小时57分43秒
  日均游戏时长：总游戏时间/在线天数
-->
```SQL
id,                               --自定义ID字段
player_id,                        --玩家id
player_name,                      --玩家名称
seconds,                          --游戏时间（秒）
avg_days,                         --日均游戏时长（秒）
```

### adm_player_game_stages：玩家游戏在线时段分布
<!-- 
  所有玩家在线时间段分布
  例如：1a：35，67，51；2a：54，80，67
  时间区域，1小时统计一次。(10分钟做一个维度计算)
-->
```SQL
id,                               --自定义ID字段
stages,                           --游戏时段
player_count_min,                 --游戏最低人数
player_count_max,                 --游戏最高人数
player_count_avg,                 --游戏平均人数
```
<!-- duration.html end -->

<!-- day.html start 游戏统计-->
### adm_kill_death_stats：玩家击杀/死亡数据统计
<!-- 
  所有玩家每天的击杀数据，与死亡数据
  时间区域，1天统计一次。
-->
```SQL
id,                               --自定义ID字段
date,                             --日期，格式：yyyy-mm-dd
player_kills,                     --击杀数据
player_deaths,                    --死亡数据
```

### adm_player_weapon_stats：玩家使用武器统计
<!-- 
  所有玩家每天武器使用情况
  时间区域，1天统计一次。
-->
```SQL
id,                               --自定义ID字段
weapon_id,                        --武器ID（adm_wepons_stats）
date,                             --日期，格式：yyyy-mm-dd
weapon_count,                     --武器使用次数（击杀次数）
weapon_name,                      --武器名称
```

### adm_map_mode_stats：比赛使用地图/模式
<!-- 
  所有数据，使用的地图、模式，占比
  例如：erangel：37835，FPS：59837，miramar：23235
-->
```SQL
id,                               --自定义ID字段
name,                             --地图/模式名称
type,                             --该数据代表的是地图还是模式（MAP/MODE）
user_count,                       --使用次数
```

### adm_player_match_stats：玩家比赛信息统计
<!-- 
  统计每一场比赛的玩家数据（gdm_agg_match_stats_pageview）
-->
```SQL
id,                               --自定义ID字段
date,                             --分区字段，格式：yyyy-mm-dd
time,                             --[date]处理字段，格式：hh-MM-ss
match_id,                         --匹配ID（adm_match_stats）
match_mode,                       --比赛模式
player_name,                      --玩家名字
player_id,                        --玩家id
maximum_distance_shot,            --最远距离射击
player_assists,                   --玩家助攻
player_dbno,                      --玩家倒下但未阵亡
player_dist_ride,                 --玩家使用载具距离
player_dist_walk,                 --玩家步行距离
player_dmg,                       --玩家伤害值（damage）
player_kills,                     --玩家击杀数
player_suvive_time,               --玩家生存时间（秒）
match_score,                      --此局评分（0.0 ~ 10.0）
```

### adm_kill_match_stats：比赛击杀信息统计
<!-- 
  统计每一场比赛的玩家数据（gdm_kill_match_stats_pageview）
-->
```SQL
id,                               --自定义ID字段
date,                             --分区字段，格式：yyyy-mm-dd
match_id,                         --匹配ID（adm_match_stats）
times,                             --击杀时间（开局到击杀发生的秒数）
killed_by,                        --击杀方式
killer_name,                      --击杀人名字
killer_id,                        --击杀人id
killer_placement,                 --击杀人位置（玩家列表位置）
killer_position_x,                --击杀位置X坐标
killer_position_y,                --击杀位置Y坐标
killer_position_600_x,            --击杀位置X坐标（比例缩放600）
killer_position_600_y,            --击杀位置Y坐标（比例缩放600）
killer_position_800_x,            --击杀位置X坐标（比例缩放800）
killer_position_800_y,            --击杀位置Y坐标（比例缩放800）
map_name,                         --击杀出现的地图
victim_name,                      --受害者名称
victim_id,                        --受害者id
victim_placement,                 --受害者位置（玩家列表位置）
victim_position_x,                --受害者位置X坐标
victim_position_y,                --受害者位置Y坐标
victim_position_600_x,            --受害者位置X坐标（比例缩放600）
victim_position_600_y,            --受害者位置Y坐标（比例缩放600）
victim_position_800_x,            --受害者位置X坐标（比例缩放800）
victim_position_800_y,            --受害者位置Y坐标（比例缩放800）
shot_distance,                    --射击距离（m）
```
<!-- day.html end -->

<!-- index.html start 首页-->
<!-- 首页的数据由其他，生涯数据，游戏统计数据构成 -->

### adm_agg_data_stats：数据聚合统计
<!-- 
  数据校准统计
-->
```SQL
id,                               --自定义ID字段
prefix_msg,                       --前缀显示信息
data_count,                       --数据统计
suffix_msg,                       --后缀显示信息
```

### adm_single_rankings_stats：单局游戏排行榜
<!-- 
  数据校准统计
-->
```SQL
id,                               --自定义ID字段
label,                            --排行内容（例如：存活最长的玩家）
player_name,                      --玩家名称
player_id,                        --玩家ID（adm_player_career）
```
<!-- index.html end -->

<!-- achievements.html start 玩家成就-->
### adm_achievements_list_stats：玩家成就列表统计
<!-- 
  统计所有玩家的成就获得比例
  例如：xingcunzhe，幸存者，54%；
-->
```SQL
id,                               --自定义ID字段
icon,                             --成就图标
name,                             --成就名称
ratio,                            --成就比例
```

### adm_achievements_days_stats：玩家成就每日统计
<!-- 
  统计所有玩家的每天获取的成就数量
-->
```SQL
id,                               --自定义ID字段
date,                             --成就获得日期，格式：yyyy-mm-dd
name,                             --成就名称
aes_count，                       --成就数量统计
```
<!-- achievements.html end -->

<!-- map.html start 热点地图-->
### adm_player_deatch_heatmap：玩家死亡热点图
<!-- 
  玩家死亡区域坐标热点。（这个估计要加redis了）
-->
```SQL
id,                               --自定义ID字段
times,                             --死亡（击杀）时间（开局到击杀发生的秒数）
map,                              --死亡出现的地图
killed_by,                        --击杀方式（武器/毒圈）
deatch_postion_x,                 --位置X坐标（受害者）
deatch_postion_y,                 --位置Y坐标（受害者）
deatch_postion_600_x,                 --位置X坐标（受害者）
deatch_postion_600_y,                 --位置Y坐标（受害者）
deatch_postion_800_x,                 --位置X坐标（受害者）
deatch_postion_800_y,                 --位置Y坐标（受害者）
```
<!-- map.html end -->

<!-- rankings.html start 排行榜-->
### adm_player_rankings_stats：玩家排行榜
<!-- 
  所有玩家 排行榜（排行算法如何写？）
  https://www.biaodianfu.com/imdb-rank.html
-->
```SQL
id,                               --自定义ID字段
player_score,                     --玩家评分
rank,                             --玩家排名（sort）
player_name,                      --玩家名称
player_id,                        --玩家ID
matchs,                           --参与了多少局比赛
kdr,                              --击杀/死亡比
dmg,                              --平均伤害（展示百分比阶段1000封顶）
kills,                            --总击杀数
victories,                        --游戏胜率（百分比）
top10,                            --top10率（百分比）
```
<!-- rankings.html end -->



<!-- weapons.html start 武器分析-->
### adm_wepons_stats：武器分析
<!-- 
  武器击杀统计，以及武器性能（性能为定死的数据）
  基于：gdm_wepons_model
-->
```SQL
id,                               --自定义ID字段
name,                             --武器名称
kdr,                              --使用武器的K/D比
preference,                       --玩家偏好（击杀出现率）
avg_kill_distance,                --平均击杀距离
damage,                           --伤害
rate_of_fire,                     --射速
reload_duration,                  --换弹时间
body_hit_impact,                  --后坐力
deatil_id,                        --明细分析ID（adm_weapons_assay）
```
<!-- weapons.html end -->

---

<!-- player.html start 玩家生涯-->
### adm_player_career：玩家生涯
<!-- 
  
-->
```SQL
id,                               --自定义ID字段

```
<!-- player.html end -->

<!-- match.html start 比赛详情-->
### adm_match_stats：玩家比赛信息统计
<!-- 
  统计每一场比赛的玩家数据（gdm_kill_match_stats_pageview）
-->
```SQL
id,                               --自定义ID字段

```
<!-- match.html end -->

<!-- weapons_assay.html start 武器明细-->
### adm_weapons_assay：武器详细信息统计
<!-- 
  
-->
```SQL
id,                               --自定义ID字段

```
<!-- weapons_assay.html end -->
