-- 玩家击杀/死亡数据统计
DROP TABLE IF EXISTS `adm_kill_death_stats`;
CREATE TABLE `adm_kill_death_stats` (
    `id` int(11) NOT NULL AUTO_INCREMENT,
    `date` date NOT NULL, 
    `player_kills` int(10) NOT NULL,
    `player_deaths` int(10) NOT NULL,
    PRIMARY KEY (`id`)
) ENGINE=InnoDB  DEFAULT CHARSET=utf8;

