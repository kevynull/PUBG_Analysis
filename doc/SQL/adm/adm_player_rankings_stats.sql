-- 玩家排行榜
DROP TABLE IF EXISTS `adm_player_rankings_stats`;
CREATE TABLE `adm_player_rankings_stats` (
    `id` int(11) NOT NULL AUTO_INCREMENT,
    `player_score` double NOT NULL,   -- 死亡（击杀）时间（开局到击杀发生的秒数）
    `rank` int(10) NOT NULL, 
    `player_name` varchar(20) NOT NULL,
    `player_id` int(11) ,
    `matchs` int(10) NOT NULL,
    `kdr` double NOT NULL,
    `dmg` double NOT NULL,
    `kills` int(10) NOT NULL,
    `victories` double NOT NULL,
    `top10` double NOT NULL,
    PRIMARY KEY (`id`)
) ENGINE=InnoDB  DEFAULT CHARSET=utf8;

