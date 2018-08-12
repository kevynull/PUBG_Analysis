-- 玩家比赛信息统计
DROP TABLE IF EXISTS `adm_player_match_stats`;
CREATE TABLE `adm_player_match_stats` (
    `id` int(11) NOT NULL AUTO_INCREMENT,
    `date` date NOT NULL, 
    `time` time NOT NULL,
    `match_id` int(11) ,  -- adm_match_stats id
    `match_mode` varchar(3) NOT NULL, 
    `player_name` varchar(20) NOT NULL, 
    `player_id` int(11) , 
    `maximum_distance_shot` double NOT NULL, 
    `player_assists` int(10) NOT NULL, 
    `player_dbno` int(10) NOT NULL, 
    `player_dist_ride` double NOT NULL, 
    `player_dist_walk` double NOT NULL, 
    `player_dmg` int(10) NOT NULL, 
    `player_kills` int(10) NOT NULL, 
    `player_suvive_time` int(10) NOT NULL, 
    `match_score` double NOT NULL, 
    PRIMARY KEY (`id`)
) ENGINE=InnoDB  DEFAULT CHARSET=utf8;

