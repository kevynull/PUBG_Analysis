-- 数据聚合统计
DROP TABLE IF EXISTS `adm_agg_data_stats`;
CREATE TABLE `adm_agg_data_stats` (
    `id` int(11) NOT NULL AUTO_INCREMENT,
    `prefix_msg` varchar(50) NOT NULL, 
    `prefix_msg` int(10) NOT NULL, 
    `suffix_msg` varchar(50) NOT NULL, 
    PRIMARY KEY (`id`)
) ENGINE=InnoDB  DEFAULT CHARSET=utf8;

