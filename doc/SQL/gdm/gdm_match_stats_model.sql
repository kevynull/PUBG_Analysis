create table gdm_match_stats_model(
`time` STRING,                             
`match_time` INT,                                                  
`match_size` INT,                       
`match_mode` STRING,                      
`max_kills` INT,                     
`player_rides_size` INT,                    
`max_dist_ride` DOUBLE,                  
`max_dist_walk` DOUBLE,                  
`max_dmg` INT,                        
`maximum_distance_shot` DOUBLE,            
`md_shot_name` STRING,
`winner_list` STRING           
)
partitioned by (`date` STRING)