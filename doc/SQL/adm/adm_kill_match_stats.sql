-- 比赛击杀信息统计
DROP TABLE IF EXISTS `adm_kill_match_stats`;
CREATE TABLE `adm_kill_match_stats` (
    `id` int(11) NOT NULL AUTO_INCREMENT,
    `date` date NOT NULL, 
    `times` int(10) NOT NULL,
    `match_id` int(11) ,  -- adm_match_stats id
    `killed_by` varchar(10) NOT NULL, 
    `killer_name` varchar(20) NOT NULL, 
    `killer_id` int(11) , 
    `killer_placement` int(2) NOT NULL, 
    `killer_position_x` double NOT NULL, 
    `killer_position_y` double NOT NULL, 
    `killer_position_600_x` double NOT NULL, 
    `killer_position_600_y` double NOT NULL, 
    `killer_position_800_x` double NOT NULL, 
    `killer_position_800_y` double NOT NULL, 
    `map_name` varchar(10) NOT NULL,
    `victim_by` varchar(10) NOT NULL, 
    `victim_name` varchar(20) NOT NULL, 
    `victim_id` int(11) , 
    `victim_placement` int(2) NOT NULL, 
    `victim_position_x` double NOT NULL, 
    `victim_position_y` double NOT NULL, 
    `victim_position_600_x` double NOT NULL, 
    `victim_position_600_y` double NOT NULL, 
    `victim_position_800_x` double NOT NULL, 
    `victim_position_800_y` double NOT NULL, 
    `shot_distance` double NOT NULL,
    PRIMARY KEY (`id`)
) ENGINE=InnoDB  DEFAULT CHARSET=utf8;

