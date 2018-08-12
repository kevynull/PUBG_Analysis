-- 比赛使用地图/模式
DROP TABLE IF EXISTS `adm_map_mode_stats`;
CREATE TABLE `adm_map_mode_stats` (
    `id` int(11) NOT NULL AUTO_INCREMENT,
    `name` varchar(10) NOT NULL, 
    `type` enum('MAP','MODE') NOT NULL,
    `user_count` int(10) NOT NULL,
    PRIMARY KEY (`id`)
) ENGINE=InnoDB  DEFAULT CHARSET=utf8;

