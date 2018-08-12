-- 玩家成就列表统计
DROP TABLE IF EXISTS `adm_achievements_list_stats`;
CREATE TABLE `adm_achievements_list_stats` (
    `id` int(11) NOT NULL AUTO_INCREMENT,
    `icon` varchar(20) NOT NULL, 
    `name` varchar(20) NOT NULL, 
    `ratio` double NOT NULL,
    PRIMARY KEY (`id`)
) ENGINE=InnoDB  DEFAULT CHARSET=utf8;

