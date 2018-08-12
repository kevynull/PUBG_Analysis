-- 玩家成就每日统计
DROP TABLE IF EXISTS `adm_achievements_days_stats`;
CREATE TABLE `adm_achievements_days_stats` (
    `id` int(11) NOT NULL AUTO_INCREMENT,
    `date` date NOT NULL, 
    `name` varchar(20) NOT NULL, 
    `aes_count` int(10) NOT NULL,
    PRIMARY KEY (`id`)
) ENGINE=InnoDB  DEFAULT CHARSET=utf8;

