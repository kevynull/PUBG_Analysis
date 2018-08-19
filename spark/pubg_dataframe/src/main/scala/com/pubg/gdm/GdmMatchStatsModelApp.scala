package com.pubg.gdm

import com.pubg.base.GdmMatchStatsModel
import com.pubg.base.util.ConfigUtil
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  *
  */
object GdmMatchStatsModelApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .enableHiveSupport()
      .getOrCreate()

    val aggTableName = ConfigUtil.DB_NAME + "." + ConfigUtil.FDM_AGG_MATCH_WIDE
    val killTableName = ConfigUtil.DB_NAME + "." + ConfigUtil.FDM_KILL_MATCH_WIDE
    val targetTableName = ConfigUtil.DB_NAME + "." + ConfigUtil.GDM_MATCH_STATS_MODEL

    val aggWide = spark.table(aggTableName)
    val killWide = spark.table(killTableName)

    val maxKills = maxKillsDF(spark, aggWide)
    val maxAssists = maxAssistsDF(spark, aggWide)
    val maxDistRide = maxDistRideDF(spark, aggWide)
    val maxDistWalk = maxDistWalkDF(spark, aggWide)
    val maxDmg = maxDmgDF(spark, aggWide)
    val mdShot = mdShotDF(spark, killWide)
    val winnerList = winnerListDF(spark, aggWide)
    val matchTime = matchTimeDF(spark, aggWide)

    /* 统计比赛通用信息 */
    val matchStats = aggWide.groupBy("date", "time", "match_id", "game_size", "party_size", "match_mode")
      .agg(sum("is_use_ride").as("player_rides_size"), count("player_name").as("player_size"))

    val gdmMatchStats = matchStats.join(maxKills, matchStats.col("match_id") === maxKills.col("pubg_opgg_id"), "left")
      .join(maxAssists, matchStats.col("match_id") === maxAssists.col("pubg_opgg_id"), "left")
      .join(maxDistRide, matchStats.col("match_id") === maxDistRide.col("pubg_opgg_id"), "left")
      .join(maxDistWalk, matchStats.col("match_id") === maxDistWalk.col("pubg_opgg_id"), "left")
      .join(maxDmg, matchStats.col("match_id") === maxDmg.col("pubg_opgg_id"), "left")
      .join(mdShot, matchStats.col("match_id") === mdShot.col("pubg_opgg_id"), "left")
      .join(winnerList, matchStats.col("match_id") === winnerList.col("pubg_opgg_id"), "left")
      .join(matchTime, matchStats.col("match_id") === matchTime.col("pubg_opgg_id"), "left")

    import spark.implicits._
    gdmMatchStats.map(line => {
      val date = line.getAs[String]("date")
      val time = line.getAs[String]("time")
      val match_time = line.getAs[Double]("match_time").toInt
      val pubg_opgg_id = line.getAs[String]("match_id")
      val match_size = line.getAs[Int]("game_size")
      val party_size = line.getAs[Int]("party_size")
      val player_size = line.getAs[Long]("player_size").toInt
      val match_mode = line.getAs[String]("match_mode")
      val max_kills = line.getAs[Int]("max_kills")
      val max_kills_name = line.getAs[String]("max_kills_name")
      val max_assists = line.getAs[Int]("max_assists")
      val max_assists_name = line.getAs[String]("max_assists_name")
      val player_rides_size = line.getAs[Long]("player_rides_size").toInt
      val max_dist_ride = line.getAs[Double]("max_dist_ride")
      val max_dist_ride_name = line.getAs[String]("max_dist_ride_name")
      val max_dist_walk = line.getAs[Double]("max_dist_walk")
      val max_dist_walk_name = line.getAs[String]("max_dist_walk_name")
      val max_dmg = line.getAs[Int]("max_dmg")
      val max_dmg_name = line.getAs[String]("max_dmg_name")
      val maximum_distance_shot = line.getAs[Double]("maximum_distance_shot")
      val md_shot_name = line.getAs[String]("md_shot_name")
      val list = line.getList[String](line.fieldIndex("winner_list"))
      var winner_list = ""
      try {
        if (!Option(list).get.isEmpty) {
          winner_list = list.toArray.mkString(";")
        }
      } catch {
        case e: Exception => {
          e.printStackTrace()
          println(list == None)
          println(list == Nil)
          println(list == null)
          println(list == Some)
        }
      }


      GdmMatchStatsModel(date, time, pubg_opgg_id, match_time, match_size, party_size, player_size, match_mode, max_kills, max_kills_name,
        max_assists, max_assists_name, player_rides_size, max_dist_ride, max_dist_ride_name, max_dist_walk,
        max_dist_walk_name, max_dmg, max_dmg_name, maximum_distance_shot, md_shot_name, winner_list)
    }).write.mode(SaveMode.Overwrite).partitionBy(ConfigUtil.PARTITION).saveAsTable(targetTableName)

  }

  /**
    * 分组取TopN，得到每局游戏，击杀最多玩家
    *
    * @param spark
    * @param aggWide
    * @return
    */
  def maxKillsDF(spark: SparkSession, aggWide: DataFrame): DataFrame = {
    import spark.implicits._
    val w = Window.partitionBy($"match_id").orderBy($"player_kills".desc)
    val maxKills = aggWide.withColumn("kill_rank", row_number().over(w)).where($"kill_rank" === 1)
      .select($"match_id".as("pubg_opgg_id"), $"player_name".as("max_kills_name"), $"player_kills".as("max_kills"))
    maxKills
  }

  /**
    * 分组取TopN，得到每局游戏，助攻最多玩家
    *
    * @param spark
    * @param aggWide
    * @return
    */
  def maxAssistsDF(spark: SparkSession, aggWide: DataFrame): DataFrame = {
    import spark.implicits._
    val w = Window.partitionBy($"match_id").orderBy($"player_assists".desc)
    val maxAssists = aggWide.withColumn("assists_rank", row_number().over(w)).where($"assists_rank" === 1)
      .select($"match_id".as("pubg_opgg_id"), $"player_name".as("max_assists_name"), $"player_assists".as("max_assists"))
    maxAssists
  }

  /**
    * 分组取TopN，得到每局游戏，飙车最远的玩家
    *
    * @param spark
    * @param aggWide
    * @return
    */
  def maxDistRideDF(spark: SparkSession, aggWide: DataFrame): DataFrame = {
    import spark.implicits._
    val w = Window.partitionBy($"match_id").orderBy($"player_dist_ride".desc)
    val maxDistRide = aggWide.withColumn("dr_rank", row_number().over(w)).where($"dr_rank" === 1)
      .select($"match_id".as("pubg_opgg_id"), $"player_name".as("max_dist_ride_name"), $"player_dist_ride".as("max_dist_ride"))
    maxDistRide
  }

  /**
    * 分组取TopN，得到每局游戏，走路最远的玩家
    *
    * @param spark
    * @param aggWide
    * @return
    */
  def maxDistWalkDF(spark: SparkSession, aggWide: DataFrame): DataFrame = {
    import spark.implicits._
    val w = Window.partitionBy($"match_id").orderBy($"player_dist_walk".desc)
    val maxDistWalk = aggWide.withColumn("dw_rank", row_number().over(w)).where($"dw_rank" === 1)
      .select($"match_id".as("pubg_opgg_id"), $"player_name".as("max_dist_walk_name"), $"player_dist_walk".as("max_dist_walk"))
    maxDistWalk
  }

  /**
    * 分组取TopN，得到每局游戏，照成伤害最多的玩家
    *
    * @param spark
    * @param aggWide
    * @return
    */
  def maxDmgDF(spark: SparkSession, aggWide: DataFrame): DataFrame = {
    import spark.implicits._
    val w = Window.partitionBy($"match_id").orderBy($"player_dmg".desc)
    val maxDmg = aggWide.withColumn("pd_rank", row_number().over(w)).where($"pd_rank" === 1)
      .select($"match_id".as("pubg_opgg_id"), $"player_name".as("max_dmg_name"), $"player_dmg".as("max_dmg"))
    maxDmg
  }

  /**
    * 分组取TopN，得到每局游戏，最远击杀的玩家
    *
    * @param spark
    * @param killWide
    * @return
    */
  def mdShotDF(spark: SparkSession, killWide: DataFrame): DataFrame = {
    import spark.implicits._
    val w = Window.partitionBy($"match_id").orderBy($"shot_distance".desc)
    val mdShot = killWide.filter($"killer_name".isNotNull)
      .withColumn("dr_rank", row_number().over(w)).where($"dr_rank" === 1)
      .select(
        $"match_id".as("pubg_opgg_id"),
        $"killer_name".as("md_shot_name"),
        $"shot_distance".as("maximum_distance_shot")
      )
    mdShot
  }

  /**
    * 分组查询，得到每局游戏的胜利玩家
    *
    * @param spark
    * @param aggWide
    * @return
    */
  def winnerListDF(spark: SparkSession, aggWide: DataFrame): DataFrame = {
    import spark.implicits._
    val wlDF = aggWide.filter($"team_placement" === 1)
      .groupBy("match_id").agg(collect_set("player_name").as("winner_list"))
      .select($"match_id".as("pubg_opgg_id"), $"winner_list")
    wlDF
  }

  /**
    * 通过最大玩家生存时间，得到比赛进行时间
    *
    * @param spark
    * @param aggWide
    * @return
    */
  def matchTimeDF(spark: SparkSession, aggWide: DataFrame): DataFrame = {
    import spark.implicits._
    val mtDF = aggWide.groupBy("match_id").agg(max("player_suvive_time").as("match_time"))
      .select($"match_id".as("pubg_opgg_id"), $"match_time")
    mtDF
  }
}
