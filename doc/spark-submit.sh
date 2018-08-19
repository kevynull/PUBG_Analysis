spark-submit \
--class com.kv.demo.SaveHiveTable \
--name SaveHiveTable \
--master yarn-cluster \
--executor-memory 1G \
--num-executors 1 \
--files /usr/lib/server/spark-2.0.2-bin-hadoop2.7/conf/hive-site.xml \
--conf spark.io.compression.codec=lz4 \
/root/jars/pubg_dataframe-1.0-SNAPSHOT.jar


spark-submit \
--class com.pubg.bdm.BdmLoadDataForCSV \
--name BdmLoadDataForCSV \
--master yarn-cluster \
--executor-memory 2G \
--num-executors 1 \
--files /usr/lib/server/spark-2.0.2-bin-hadoop2.7/conf/hive-site.xml \
--conf spark.io.compression.codec=lz4 \
/root/jars/pubg_dataframe-1.0-SNAPSHOT.jar

# spark-submit \
# --class com.pubg.fdm.FdmLoadDataForBdm \
# --name FdmLoadDataForBdm \
# --master yarn-cluster \
# --executor-memory 2G \
# --num-executors 1 \
# --files /usr/lib/server/spark-2.0.2-bin-hadoop2.7/conf/hive-site.xml \
# --conf spark.io.compression.codec=lz4 \
# /root/jars/pubg_dataframe-1.0-SNAPSHOT.jar

spark-submit \
--class com.pubg.fdm.FdmAggMatchWideApp \
--name FdmAggMatchWideApp \
--master yarn-cluster \
--executor-memory 2G \
--num-executors 1 \
--files /usr/lib/server/spark-2.0.2-bin-hadoop2.7/conf/hive-site.xml \
--conf spark.io.compression.codec=lz4 \
/root/jars/pubg_dataframe-1.0-SNAPSHOT.jar

spark-submit \
--class com.pubg.fdm.FdmKillMatchWideApp \
--name FdmKillMatchWideApp \
--master yarn-cluster \
--executor-memory 2G \
--num-executors 1 \
--files /usr/lib/server/spark-2.0.2-bin-hadoop2.7/conf/hive-site.xml \
--conf spark.io.compression.codec=lz4 \
--conf spark.yarn.executor.memoryOverhead=1024 \
/root/jars/pubg_dataframe-1.0-SNAPSHOT.jar

spark-submit \
--class com.pubg.gdm.GdmPlayerMatchStatsPageviewApp \
--name GdmPlayerMatchStatsPageviewApp \
--master yarn-cluster \
--executor-memory 2G \
--num-executors 1 \
--files /usr/lib/server/spark-2.0.2-bin-hadoop2.7/conf/hive-site.xml \
--conf spark.io.compression.codec=lz4 \
/root/jars/pubg_dataframe-1.0-SNAPSHOT.jar

spark-submit \
--class com.pubg.gdm.GdmKillMatchStatsPageviewApp \
--name GdmKillMatchStatsPageviewApp \
--master yarn-cluster \
--executor-memory 2G \
--num-executors 1 \
--files /usr/lib/server/spark-2.0.2-bin-hadoop2.7/conf/hive-site.xml \
--conf spark.io.compression.codec=lz4 \
/root/jars/pubg_dataframe-1.0-SNAPSHOT.jar

spark-submit \
--class com.pubg.gdm.GdmMatchStatsModelApp \
--name GdmMatchStatsModelApp \
--master yarn-cluster \
--executor-memory 2G \
--num-executors 1 \
--files /usr/lib/server/spark-2.0.2-bin-hadoop2.7/conf/hive-site.xml \
--conf spark.io.compression.codec=lz4 \
/root/jars/pubg_dataframe-1.0-SNAPSHOT.jar


# --conf spark.io.compression.codec=lz4 启用压缩，减小内存使用量
# 
# ??--files /etc/hive/conf/hive-site.xml

spark-submit \
--class com.kv.demo.SaveHiveTable \
--name SaveHiveTable \
--master spark://node-0:7077 \
--executor-memory 1G \
--num-executors 1 \
/root/jars/pubg_dataframe-1.0-SNAPSHOT-jar-with-dependencies.jar


# 备注：当使用spark on yarn 时， spark程序并没有hive-site.xml这个配置项，所以无法访问hive

--driver-java-options "-Dlog4j.configuration=/root/conf/log4j.properties"


########### 测试SQL

#select agg_list.date, agg_list.match_id, win_list.winner_list from (select date, match_id from fdm_agg_match_wide group by date, match_id) agg_list left join (select match_id , concat_ws(';', collect_set(player_name)) as winner_list from fdm_agg_match_wide group by match_id) win_list on famw.match_id = win_list.match_id where win_list.winner is null limit 10;


# gdm_match_stats_model 中的数据与 fdm_agg_match_wide中的数据不符合，缺少了315条， 经过查明，fdm_agg_match_wide 中 有数据 ，player_name为空，在程序执行时过滤掉了。
#select count(1) from fdm_agg_match_wide where match_id in (select distinct pubg_opgg_id from gdm_match_stats_model where length(winner_list) = 0) and team_placement = 1


# select count(hour), hour , player_name from fdm_agg_match_wide where player_name is not null group by player_name,hour order by player_name asc ,count(hour) desc, hour desc limit 50;