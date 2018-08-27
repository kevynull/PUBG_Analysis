package com.kv.demo

import com.pubg.base.BdmKillMatchStats
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{count, row_number}

object WindowsFunTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("test")
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._
    val path = "file:///D://code//DataBase//kaggle//pubg-match-deaths//deaths"

    //val people = spark.read.option("header","true").csv(path)
    val kill = spark.read.option("header","true").option("inferSchema","false").csv(path)

    val killGroup = kill.where($"killer_name".isNotNull)
      .groupBy($"killer_name",$"time")
      .agg(count($"time").as("time_count"))

    killGroup.show()

    import spark.implicits._
    val w = Window.partitionBy($"killer_name").orderBy($"time".desc)
    killGroup.withColumn("hour_rank", row_number().over(w)).where($"hour_rank" === 1)
      .select(
        $"killer_name".as("player"),
        $"time".as("online_stages")
      ).show()
  }
}
