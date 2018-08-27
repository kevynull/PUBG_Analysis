create table gdm_match_stats_model(
`time` STRING,                             
`match_time` DOUBLE,    
`pubg_opgg_id` STRING,                                
`match_size` INT,      
`party_size` INT,  
`player_size` INT,               
`match_mode` STRING,                      
`max_kills` INT,  
`max_kills_name` STRING,         
`max_assists` INT,                      --最大助攻数（此局）     
`max_assists_name` STRING,                 --最大助攻数 玩家名称（此局）          
`player_rides_size` INT,                    
`max_dist_ride` DOUBLE,      
`max_dist_ride_name` STRING,               --比赛中使用载具最大距离 玩家名称            
`max_dist_walk` DOUBLE,       
`max_dist_walk_name`STRING,               --比赛中步行最大距离 玩家名称           
`max_dmg` INT,                 
`max_dmg_name`STRING,                     --比赛中玩家照成的最大伤害 玩家名称       
`maximum_distance_shot` DOUBLE,            
`md_shot_name` STRING,
`winner_list` STRING           
)
partitioned by (`date` STRING)