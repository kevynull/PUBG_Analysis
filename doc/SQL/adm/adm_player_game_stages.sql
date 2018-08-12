-- 玩家游戏在线时段分布
DROP TABLE IF EXISTS `adm_player_game_stages`;
CREATE TABLE `adm_player_game_stages` (
    `id` int(11) NOT NULL AUTO_INCREMENT,
    `stages` varchar(5) NOT NULL, 
    `player_name` varchar(20) NOT NULL,
    `player_count_min` int(10) NOT NULL,
    `player_count_max` int(10) NOT NULL,
    `player_count_avg` double NOT NULL,
    PRIMARY KEY (`id`)
) ENGINE=InnoDB  DEFAULT CHARSET=utf8;

