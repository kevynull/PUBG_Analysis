# spark-submit \
# --class com.kv.demo.SaveHiveTable \
# --name SaveHiveTable \
# --master yarn-cluster \
# --executor-memory 1G \
# --num-executors 1 \
# --files /usr/lib/server/spark-2.0.2-bin-hadoop2.7/conf/hive-site.xml \
# --conf spark.io.compression.codec=lz4 \
# /root/jars/pubg_dataframe-1.0-SNAPSHOT.jar

# spark-submit \
# --class com.pubg.fdm.FdmLoadDataForBdm \
# --name FdmLoadDataForBdm \
# --master yarn-cluster \
# --executor-memory 2G \
# --num-executors 1 \
# --files /usr/lib/server/spark-2.0.2-bin-hadoop2.7/conf/hive-site.xml \
# --conf spark.io.compression.codec=lz4 \
# /root/jars/pubg_dataframe-1.0-SNAPSHOT.jar

##
# 将csv中的内容导入到hive中
# bdm_agg_match_stats
##
spark-submit \
--class com.pubg.bdm.BdmAggMatchStatsApp \
--name BdmAggMatchStatsApp \
--master yarn \
--deploy-mode cluster \
--executor-memory 2G \
--num-executors 2 \
--files /usr/lib/server/spark-2.0.2-bin-hadoop2.7/conf/hive-site.xml \
--conf spark.io.compression.codec=lz4 \
/root/jars/pubg_dataframe-1.0-SNAPSHOT.jar \
hdfs://node001:9000/pubg_data/aggregate

##
# 将csv中的内容导入到hive中
# bdm_kill_match_stats
##
spark-submit \
--class com.pubg.bdm.BdmKillMatchStatsApp \
--name BdmKillMatchStatsApp \
--master yarn \
--deploy-mode cluster \
--executor-memory 2G \
--num-executors 2 \
--files /usr/lib/server/spark-2.0.2-bin-hadoop2.7/conf/hive-site.xml \
--conf spark.io.compression.codec=lz4 \
/root/jars/pubg_dataframe-1.0-SNAPSHOT.jar \
hdfs://node001:9000/pubg_data/deaths

##
# bdm层中的数据，扩宽清洗。存储到fdm层
# fdm_agg_match_wide
##
spark-submit \
--class com.pubg.fdm.FdmAggMatchWideApp \
--name FdmAggMatchWideApp \
--master yarn \
--deploy-mode cluster \
--executor-memory 2G \
--num-executors 2 \
--files /usr/lib/server/spark-2.0.2-bin-hadoop2.7/conf/hive-site.xml \
--conf spark.io.compression.codec=lz4 \
/root/jars/pubg_dataframe-1.0-SNAPSHOT.jar

##
# bdm层中的数据，扩宽清洗。存储到fdm层
# fdm_kill_match_wide
##
spark-submit \
--class com.pubg.fdm.FdmKillMatchWideApp \
--name FdmKillMatchWideApp \
--master yarn \
--deploy-mode cluster \
--executor-memory 2G \
--num-executors 1 \
--files /usr/lib/server/spark-2.0.2-bin-hadoop2.7/conf/hive-site.xml \
/root/jars/pubg_dataframe-1.0-SNAPSHOT.jar

# --driver-memory 2g \
# --conf spark.io.compression.codec=lz4 \

##
# bdm层中的数据，聚合统计。存储到gdm层
# gdm_player_match_stats_pageview
##
spark-submit \
--class com.pubg.gdm.GdmPlayerMatchStatsPageviewApp \
--name GdmPlayerMatchStatsPageviewApp \
--master yarn \
--deploy-mode cluster \
--executor-memory 2G \
--num-executors 2 \
--files /usr/lib/server/spark-2.0.2-bin-hadoop2.7/conf/hive-site.xml \
--conf spark.io.compression.codec=lz4 \
/root/jars/pubg_dataframe-1.0-SNAPSHOT.jar

##
# bdm层中的数据，聚合统计。存储到gdm层
# gdm_kill_match_stats_pageview
##
spark-submit \
--class com.pubg.gdm.GdmKillMatchStatsPageviewApp \
--name GdmKillMatchStatsPageviewApp \
--master yarn-cluster \
--executor-memory 2G \
--num-executors 2 \
--files /usr/lib/server/spark-2.0.2-bin-hadoop2.7/conf/hive-site.xml \
--conf spark.io.compression.codec=lz4 \
/root/jars/pubg_dataframe-1.0-SNAPSHOT.jar

##
# bdm层中的数据，聚合统计。存储到gdm层
# gdm_match_stats_model
##
spark-submit \
--class com.pubg.gdm.GdmMatchStatsModelApp \
--name GdmMatchStatsModelApp \
--master yarn \
--deploy-mode cluster \
--executor-memory 2G \
--num-executors 1 \
--files /usr/lib/server/spark-2.0.2-bin-hadoop2.7/conf/hive-site.xml \
--conf spark.io.compression.codec=lz4 \
/root/jars/pubg_dataframe-1.0-SNAPSHOT.jar


# --conf spark.io.compression.codec=lz4 启用压缩，减小内存使用量
# 
# ??--files /etc/hive/conf/hive-site.xml

# spark-submit \
# --class com.kv.demo.SaveHiveTable \
# --name SaveHiveTable \
# --master spark://node-0:7077 \
# --executor-memory 1G \
# --num-executors 1 \
# /root/jars/pubg_dataframe-1.0-SNAPSHOT-jar-with-dependencies.jar


# 备注：当使用spark on yarn 时， spark程序并没有hive-site.xml这个配置项，所以无法访问hive



########### 测试SQL

#select agg_list.date, agg_list.match_id, win_list.winner_list from (select date, match_id from fdm_agg_match_wide group by date, match_id) agg_list left join (select match_id , concat_ws(';', collect_set(player_name)) as winner_list from fdm_agg_match_wide group by match_id) win_list on famw.match_id = win_list.match_id where win_list.winner is null limit 10;


# gdm_match_stats_model 中的数据与 fdm_agg_match_wide中的数据不符合，缺少了315条， 经过查明，fdm_agg_match_wide 中 有数据 ，player_name为空，在程序执行时过滤掉了。
#select count(1) from fdm_agg_match_wide where match_id in (select distinct pubg_opgg_id from gdm_match_stats_model where length(winner_list) = 0) and team_placement = 1


# select count(hour), hour , player_name from fdm_agg_match_wide where player_name is not null group by player_name,hour order by player_name asc ,count(hour) desc, hour desc limit 50;


./spark-submit \
--class com.xyz.MySpark \
--conf "spark.executor.extraJavaOptions=-XX:MaxPermSize=512M" \
--driver-java-options -XX:MaxPermSize=512m \
--driver-memory 3g \
--master yarn \
--deploy-mode cluster \
--executor-memory 2G \
--executor-cores 8 \
--num-executors 12  \
/home/myuser/myspark-1.0.jar


./spark-submit --class com.xyz.MySpark \
--conf "spark.executor.extraJavaOptions=-XX:MaxPermSize=1024M" \
--driver-java-options -XX:MaxPermSize=1024m \
--driver-memory 4g \
--master yarn-client \
--executor-memory 2G \
--executor-cores 8 \
--num-executors 15  \
/home/myuser/myspark-1.0.jar


#!/bin/bash  
source /etc/profile  
  
nohup /opt/modules/spark/bin/spark-submit \
--master spark://10.130.2.20:7077 \
--conf "spark.executor.extraJavaOptions=-XX:PermSize=8m -XX:SurvivorRatio=4 -XX:NewRatio=4 -XX:+PrintGCDetails -XX:+PrintGCTimeStamps" \
--driver-memory 1g \
--executor-memory 1g \
--total-executor-cores 48 \
--conf "spark.ui.port=8088"  \
--jars /opt/bin/sparkJars/kafka_2.10-0.8.2.1.jar,/opt/bin/sparkJars/spark-streaming-kafka_2.10-1.4.1.jar,/opt/bin/sparkJars/metrics-core-2.2.0.jar,/opt/bin/sparkJars  
/mysql-connector-java-5.1.26-bin.jar,/opt/bin/sparkJars/spark-streaming-kafka_2.10-1.4.1.jar \
--class com.spark.streaming.Top3HotProduct \
SparkApp.jar \

