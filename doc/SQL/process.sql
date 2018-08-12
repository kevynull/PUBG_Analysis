select kill.map, count(kill.map) as map_count from bdm_deaths kill 
left join bdm_aggregate agg 
on kill.match_id = agg.match_id 
group by kill.map;