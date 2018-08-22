package com.pubg.bdm

import com.pubg.base.BdmAggMatchStats
import com.pubg.base.util.ConfigUtil
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 数据缓存层
  * 读取csv数据，将CSV数据导入到hive中
  */
object BdmSourceMatchStatsApp {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Usage: BdmSourceMatchStatsApp <inputPath> <tableName>")
      System.exit(1)
    }
    val spark = SparkSession.builder()
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    val Array(inputPath, tableName) = args

    val hiveTable = ConfigUtil.DB_NAME + "." + tableName


    val agg = spark.read.option("header", "true").option("inferSchema", "true").csv(inputPath)
    agg.as[BdmAggMatchStats].write.mode(SaveMode.Overwrite).saveAsTable(hiveTable)
  }

}
