-- 玩家使用武器统计
DROP TABLE IF EXISTS `adm_player_weapon_stats`;
CREATE TABLE `adm_player_weapon_stats` (
    `id` int(11) NOT NULL AUTO_INCREMENT,
    `weapon_id` int(11) NULL,
    `date` date NOT NULL, 
    `weapon_count` int(10) NOT NULL,
    `weapon_name` varchar(20) NOT NULL,
    PRIMARY KEY (`id`)
) ENGINE=InnoDB  DEFAULT CHARSET=utf8;

