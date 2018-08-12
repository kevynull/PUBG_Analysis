-- 玩家游戏时长
DROP TABLE IF EXISTS `adm_player_online_time`;
CREATE TABLE `adm_player_online_time` (
    `id` int(11) NOT NULL AUTO_INCREMENT,
    `player_id` int(11) , 
    `player_name` varchar(20) NOT NULL,
    `seconds` int(10) NOT NULL,
    `avg_days` double NOT NULL,
    PRIMARY KEY (`id`)
) ENGINE=InnoDB  DEFAULT CHARSET=utf8;

