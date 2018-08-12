-- 玩家死亡热点图
DROP TABLE IF EXISTS `adm_player_deatch_heatmap`;
CREATE TABLE `adm_player_deatch_heatmap` (
    `id` int(11) NOT NULL AUTO_INCREMENT,
    `times` int(8) NOT NULL,   -- 死亡（击杀）时间（开局到击杀发生的秒数）
    `map` varchar(10) NOT NULL, 
    `deatch_postion_x` double NOT NULL,
    `deatch_postion_y` double NOT NULL,
    `deatch_postion_600_x` double NOT NULL,
    `deatch_postion_600_y` double NOT NULL,
    `deatch_postion_800_x` double NOT NULL,
    `deatch_postion_800_y` double NOT NULL,
    PRIMARY KEY (`id`)
) ENGINE=InnoDB  DEFAULT CHARSET=utf8;

