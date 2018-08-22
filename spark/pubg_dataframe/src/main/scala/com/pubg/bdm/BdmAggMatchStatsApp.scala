package com.pubg.bdm

import com.pubg.base.BdmAggMatchStats
import com.pubg.base.util.ConfigUtil
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 数据缓存层
  * 读取csv数据，将CSV数据导入到hive中
  */
object BdmAggMatchStatsApp {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("Usage: BdmAggMatchStatsApp <inputPath>")
      System.exit(1)
    }
    val spark = SparkSession.builder()
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    val Array(inputPath) = args

    val tableName = ConfigUtil.DB_NAME + "." + ConfigUtil.BDM_AGG_MATCH_STATS


    val agg = spark.read.option("header", "true").option("inferSchema", "true").csv(inputPath)
    agg.as[BdmAggMatchStats].coalesce(1).write.mode(SaveMode.Overwrite).saveAsTable(tableName)
  }

}
