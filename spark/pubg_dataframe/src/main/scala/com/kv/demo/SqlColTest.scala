package com.kv.demo

import com.pubg.base.TempGdmKillDistance
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * 测试同一张表，多个属性计算
  */
object SqlColTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("test")
      .master("local[2]")
      .getOrCreate()

    val path = "hdfs://node-0:9000/test_data/kill"

    sqlColDistance(spark, path)

  }

  def sqlCoordinateScale(spark:SparkSession,path:String): Unit ={
    val kill = spark.read.option("header", "true").option("inferSchema", "true").csv(path)

    import spark.implicits._

    val tempKill = kill.where(kill.col("killer_name").isNotNull).map(line => {
      val x1 = line.getAs[Double]("killer_position_x")
      val x2 = line.getAs[Double]("victim_position_x")
      val y1 = line.getAs[Double]("killer_position_y")
      val y2 = line.getAs[Double]("victim_position_y")
      val dis_shot = distance(x1,x2,y1,y2)
      TempGdmKillDistance(dis_shot,line.getAs[String]("match_id"),line.getAs[String]("killer_name"))
    }).select("distance_shot","killer_name","match_id")
  }

  def sqlColDistance(spark:SparkSession,path:String): Unit ={
    val kill = spark.read.option("header", "true").option("inferSchema", "true").csv(path)

    import spark.implicits._

    /*val tempKill = kill.where(kill.col("killer_name").isNotNull).map(line => {
      val x1 = line.getAs[Double]("killer_position_x")
      val x2 = line.getAs[Double]("victim_position_x")
      val y1 = line.getAs[Double]("killer_position_y")
      val y2 = line.getAs[Double]("victim_position_y")
      val dis_shot = distance(x1,x2,y1,y2)
      TempGdmKillDistance(dis_shot,line.getAs[String]("match_id"),line.getAs[String]("killer_name"))
    }).select("shot_distance","killer_name","match_id")

    tempKill.groupBy(tempKill.col("match_id"), tempKill.col("killer_name"))
      .agg(max(tempKill.col("shot_distance")).as("shot_distance")).show()*/


    val x1 = kill.col("killer_position_x")
    val x2 = kill.col("victim_position_x")
    val y1 = kill.col("killer_position_y")
    val y2 = kill.col("victim_position_y")
    val d = (x1 - x2) * (x1 - x2) + (y1 - y2) * (y1 - y2)

    val killGroup = kill.where(kill.col("killer_name").isNotNull)
      .groupBy(kill.col("match_id"), kill.col("killer_name"))
        .agg(max(sqrt(d)).as("shot_distance"))

    killGroup.select("match_id","killer_name","shot_distance").show()

  }

  def distance(x1: Double, x2: Double, y1: Double, y2: Double): Double = {
    val xSqr = (x1 - x2) * (x1 - x2)
    val ySqr = (y1 - y2) * (y1 - y2)
    math.sqrt(xSqr + ySqr)
  }
}
