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

spark-submit \
--class com.pubg.fdm.FdmLoadDataForBdm \
--name FdmLoadDataForBdm \
--master yarn-cluster \
--executor-memory 2G \
--num-executors 1 \
--files /usr/lib/server/spark-2.0.2-bin-hadoop2.7/conf/hive-site.xml \
--conf spark.io.compression.codec=lz4 \
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