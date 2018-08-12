-- 在一个时间段内玩家数量/游戏时间
DROP TABLE IF EXISTS `adm_player_count_stages`;
CREATE TABLE `adm_player_count_stages` (
    `id` int(11) NOT NULL AUTO_INCREMENT,
    `date` date NOT NULL, 
    `time` time NOT NULL,
    `player_count` int(10) NOT NULL,
    `game_time` int(10) NOT NULL,
    PRIMARY KEY (`id`)
) ENGINE=InnoDB  DEFAULT CHARSET=utf8;

