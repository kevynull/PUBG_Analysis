package com.kv.demo

import org.apache.spark.sql.functions.concat_ws
import org.apache.spark.sql.{SaveMode, SparkSession}

object ReadcsvAPP {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("read_csv")
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._
    val path = "file:///D:/code/DataBase/kaggle/pubg-match-deaths/aggregate/agg_match_stats_4_top50_demo.csv"

    //val people = spark.read.option("header","true").csv(path)
    val people = spark.read.option("header","true").option("inferSchema","true").csv(path)

//    people.printSchema()
//    people.show()

    val ds = people.as[AggMatchStats]
    //ds.write.mode()
    //ds.map(line => line).show
    ds.select(concat_ws(" ", $"party_size", $"game_size").as("test")).show()
    spark.stop()

  }

  case class AggMatchStats(
       date:String,
       game_size:Int,
       match_id:String,
       match_mode:String,
       party_size:Int,
       player_assists:Int,
       player_dbno:Int,
       player_dist_ride:Double,
       player_dist_walk:Double,
       player_dmg:Int,
       player_kills:Int,
       player_name:String,
       player_survive_time:Double,
       team_id:Int,
       team_placement:Int
  )
}
