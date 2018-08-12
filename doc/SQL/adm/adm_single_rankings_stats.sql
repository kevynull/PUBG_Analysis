-- 单局游戏排行榜
DROP TABLE IF EXISTS `adm_single_rankings_stats`;
CREATE TABLE `adm_single_rankings_stats` (
    `id` int(11) NOT NULL AUTO_INCREMENT,
    `label` varchar(50) NOT NULL, 
    `player_name` varchar(20) NOT NULL, 
    `player_id` int(11) ,  -- adm_player_career id
    PRIMARY KEY (`id`)
) ENGINE=InnoDB  DEFAULT CHARSET=utf8;

