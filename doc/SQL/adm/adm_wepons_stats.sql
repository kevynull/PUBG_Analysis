-- 武器分析
DROP TABLE IF EXISTS `adm_wepons_stats`;
CREATE TABLE `adm_wepons_stats` (
    `id` int(11) NOT NULL AUTO_INCREMENT,
    `name` varchar(20) NOT NULL,   
    `kdr` double NOT NULL, 
    `preference` double NOT NULL,
    `avg_kill_distance` double ,
    `damage` double NOT NULL,
    `rate_of_fire` double NOT NULL,
    `dmg` double NOT NULL,
    `reload_duration` double NOT NULL,
    `body_hit_impact` double NOT NULL,
    `deatil_id` double ,  -- adm_weapons_assay id
    PRIMARY KEY (`id`)
) ENGINE=InnoDB  DEFAULT CHARSET=utf8;

