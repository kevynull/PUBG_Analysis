package com.kv.demo

import com.pubg.base.{BdmAggMatchStats, FdmAggMatchWide}
import org.apache.spark.sql.{SaveMode, SparkSession}

object SaveHiveTable {
  def main(args: Array[String]): Unit = {
    /*val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()*/

    val spark = SparkSession.builder()
      .appName("test")
      .master("local[2]")
      .getOrCreate()

    readTable(spark)

    spark.stop()
  }

  def saveTable(spark: SparkSession): Unit ={
    val path = "hdfs://node-0:9000/test_data/agg"

    val agg = spark.read.option("header","true").option("inferSchema","true").csv(path)
    agg.show()
    agg.write.mode(SaveMode.Overwrite).saveAsTable("test_data.agg_match")
  }

  def readTable(spark: SparkSession): Unit ={
    val path = "hdfs://node-0:9000/test_data/agg"

    val agg = spark.read.option("header","true").option("inferSchema","true").csv(path)
    import spark.implicits._
    agg.as[BdmAggMatchStats].map(line =>{
      BAMS(line.date, line.game_size, 1)
    }).show()
  }

  case class BAMS(date:String, game_size:Int, number:Int)
}
