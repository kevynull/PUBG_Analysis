package com.kv.demo


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object GdmMatchStatsTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("test")
      .master("local[2]")
      .getOrCreate()

    val path = "hdfs://node-0:9000/test_data/agg"
    val kill_path = "hdfs://node-0:9000/test_data/kill"

    val agg = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
    val kill = spark.read.option("header", "true").option("inferSchema", "true").csv(kill_path)

    /**
      * GdmMatchStatsModelApp 中的查询，其实就是分组取topN的问题，df需要用到开窗函数来处理。
      */
    import spark.implicits._
    val w = Window.partitionBy($"match_id").orderBy($"player_kills".desc)
    val dfTop1 = agg.withColumn("kill_rank", row_number().over(w)).where($"kill_rank" === 1).drop("kill_rank")
    val killGroup = dfTop1.select($"match_id".as("pubg_opgg_id"), $"player_name", $"player_kills")

    val aggGroup = agg.groupBy("match_id", "game_size", "match_mode").agg(count("match_mode"))


    /* 因为 aggGroup 与 killGroup都是同一个表的数据，需要在查询出来前进行重命名 */
    val aggJoin = aggGroup.join(killGroup, aggGroup.col("match_id") === killGroup.col("pubg_opgg_id"), "left")
    //aggJoin.select("pubg_opgg_id","game_size","match_mode","player_kills","player_name").show()


    /* test win list*/
    val wlDF = agg.filter(agg.col("team_placement") === 1)
      .groupBy("match_id").agg(collect_set("player_name").as("winner_list"))
      .select("match_id", "winner_list")
    //wlDF.show()



    wlDF.map(line => {
      val list = line.getList[String](line.fieldIndex("winner_list"))
      var xx = ""
      if (list != null && !list.isEmpty){
        xx = list.toArray.mkString(";")
      }
      Test(xx)
    }).show()


    val matchStats = agg.groupBy("date", "match_id", "game_size", "match_mode")
      .agg(sum("game_size").as("player_rides_size"))
    //matchStats.show()

    //aggJoin.show()
  }

  case class Test(str:String)
}
