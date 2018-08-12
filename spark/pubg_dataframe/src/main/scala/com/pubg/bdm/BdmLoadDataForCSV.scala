package com.pubg.bdm

import com.pubg.base.util.ConfigUtil
import com.pubg.base.{BdmAggMatchStats, BdmKillMatchStats}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 数据缓存层，将CSV数据导入到hive中
  */
object BdmLoadDataForCSV {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .enableHiveSupport()
      .getOrCreate()

    /* local */
    /*val spark = SparkSession.builder()
      .appName("read_csv")
      .master("local[2]")
      .getOrCreate()*/

    loadAggMatchStatsCSV(spark, ConfigUtil.AGG_SOURCE_PATHS)

    loadKillMatchStatsCSV(spark, ConfigUtil.KILL_SOURCE_PATHS)

    spark.stop()

  }

  /**
    * 读取agg csv数据
    * @param spark
    * @param paths
    */
  def loadAggMatchStatsCSV(spark:SparkSession, paths:String): Unit ={
    val tableName = ConfigUtil.DB_NAME + "." + ConfigUtil.BDM_AGG_MATCH_STATS

    import spark.implicits._
    val agg = spark.read.option("header", "true").option("inferSchema", "true").csv(paths)
    val agg_ds = agg.as[BdmAggMatchStats]
    agg_ds.write.mode(SaveMode.Overwrite).saveAsTable(tableName)

  }

  /**
    * 读取kill csv数据
    * @param spark
    * @param paths
    */
  def loadKillMatchStatsCSV(spark:SparkSession, paths: String): Unit ={
    val tableName = ConfigUtil.DB_NAME + "." + ConfigUtil.BDM_KILL_MATCH_STATS

    import spark.implicits._
    val kill = spark.read.option("header", "true").option("inferSchema", "true").csv(paths)
    val kill_ds = kill.as[BdmKillMatchStats]
    kill_ds.write.mode(SaveMode.Overwrite).saveAsTable(tableName)

  }
}
