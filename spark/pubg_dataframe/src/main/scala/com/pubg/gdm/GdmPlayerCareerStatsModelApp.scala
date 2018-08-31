package com.pubg.gdm

import com.pubg.base.{GdmPlayerCareerStatsModel, TempGdmPlayerStats}
import com.pubg.base.util.ConfigUtil
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{row_number, _}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

/**
  *
  */
object GdmPlayerCareerStatsModelApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .enableHiveSupport()
      .getOrCreate()

    val aggTableName = ConfigUtil.DB_NAME + "." + ConfigUtil.FDM_AGG_MATCH_WIDE
    val killTableName = ConfigUtil.DB_NAME + "." + ConfigUtil.FDM_KILL_MATCH_WIDE
    val targetTableName = ConfigUtil.DB_NAME + "." + ConfigUtil.GDM_PLAYER_CAREER_STATS_MODEL

    //分区字段
    val PARTITION_BY = "first_play_date"

    /* 必须判断 player_name is not null */

    val aggWide = spark.table(aggTableName)
    val killWide = spark.table(killTableName)

    val firstPlayTime = firstPlayTimeDF(spark, aggWide)
    val lastPlayTime = lastPlayTimeDF(spark, aggWide)
    val onlineStages = onlineStagesDF(spark, aggWide)
    val maxDistShot = maxDistShotDF(spark, killWide)
    val maxSuviveTime = maxSuviveTimeDF(spark, aggWide)
    val maxKills = maxKillsDF(spark, aggWide)
    val maxAssists = maxAssistsDF(spark, aggWide)
    val top10Count = top10CountDF(spark, aggWide)
    val maxDistRide = maxDistRideDF(spark, aggWide)
    val maxDistWalk = maxDistWalkDF(spark, aggWide)
    /* 玩家聚合统计 */
    import spark.implicits._
    val playerStats = aggWide.groupBy("player_name")
      .agg(
        sum("player_kills").as("total_kills"),
        avg("player_kills").as("avg_kills"),
        sum("player_assists").as("total_assists"),
        avg("player_assists").as("avg_assists"),
        sum("player_suvive_time").as("total_suvive_time"),
        avg("player_suvive_time").as("avg_suvive_time"),
        sum("player_dmg").as("total_dmg"),
        avg("player_dmg").as("avg_dmg"),
        count("player_name").as("play_count"),
        sum("is_win").as("win_count"),
        sum("is_team").as("party_count"),
        sum("player_dbno").as("total_dbno"),
        sum("player_dist_ride").as("total_dist_ride"),
        sum("player_dist_walk").as("total_dist_walk"),
        sum("is_use_ride").as("count_use_ride")
      ).map(line => {
      val player_name = line.getAs[String]("player_name")
      val total_kills = line.getAs[Long]("total_kills").toInt
      val avg_kills = line.getAs[Double]("avg_kills")
      val total_assists = line.getAs[Long]("total_assists").toInt
      val avg_assists = line.getAs[Double]("avg_assists")
      val total_suvive_time = line.getAs[Double]("total_suvive_time")
      val avg_suvive_time = line.getAs[Double]("avg_suvive_time")
      val total_dmg = line.getAs[Long]("total_dmg").toInt
      val avg_dmg = line.getAs[Double]("avg_dmg")
      val play_count = line.getAs[Long]("play_count").toInt
      val win_count = line.getAs[Long]("win_count").toInt
      val party_count = line.getAs[Long]("party_count").toInt
      val total_dbno = line.getAs[Long]("total_dbno").toInt
      val total_dist_ride = line.getAs[Double]("total_dist_ride")
      val total_dist_walk = line.getAs[Double]("total_dist_walk")
      val count_use_ride = line.getAs[Long]("count_use_ride").toInt
      val kill_death_ratio = (total_kills + (total_assists * 0.3)) / play_count  // 计算KDA

      TempGdmPlayerStats(player_name, total_kills, avg_kills, total_assists, avg_assists, total_suvive_time, avg_suvive_time,
        total_dmg, avg_dmg, play_count, win_count, party_count, total_dbno, total_dist_ride, total_dist_walk,
        count_use_ride, kill_death_ratio)
    })
    val gdmPlayerCareerStats = playerStats
      .join(firstPlayTime, firstPlayTime.col("player") === playerStats.col("player_name"), "left")
      .join(lastPlayTime, lastPlayTime.col("player") === playerStats.col("player_name"), "left")
      .join(onlineStages, onlineStages.col("player") === playerStats.col("player_name"), "left")
      .join(maxDistShot, maxDistShot.col("player") === playerStats.col("player_name"), "left")
      .join(maxSuviveTime, maxSuviveTime.col("player") === playerStats.col("player_name"), "left")
      .join(maxKills, maxKills.col("player") === playerStats.col("player_name"), "left")
      .join(maxAssists, maxAssists.col("player") === playerStats.col("player_name"), "left")
      .join(top10Count, top10Count.col("player") === playerStats.col("player_name"), "left")
      .join(maxDistRide, maxDistRide.col("player") === playerStats.col("player_name"), "left")
      .join(maxDistWalk, maxDistWalk.col("player") === playerStats.col("player_name"), "left")

    gdmPlayerCareerStats.map(line => {
      val name = line.getAs[String]("player_name")
      val first_play_time = line.getAs[String]("first_play_time")
      val first_play_date = line.getAs[String]("first_play_date")
      val last_play_time = line.getAs[String]("last_play_time")
      val total_kills = line.getAs[Int]("total_kills")
      val avg_kills = line.getAs[Double]("avg_kills")
      val total_assists = line.getAs[Int]("total_assists")
      val avg_assists = line.getAs[Double]("avg_assists")
      val total_suvive_time = line.getAs[Double]("total_suvive_time")
      val avg_suvive_time = line.getAs[Double]("avg_suvive_time")
      val total_dmg = line.getAs[Int]("total_dmg")
      val avg_dmg = line.getAs[Double]("avg_dmg")
      val play_count = line.getAs[Int]("play_count")
      val win_count = line.getAs[Int]("win_count")
      val party_count = line.getAs[Int]("party_count")
      val total_dbno = line.getAs[Int]("total_dbno")
      val total_dist_ride = line.getAs[Double]("total_dist_ride")
      val max_dist_ride = line.getAs[Double]("max_dist_ride")
      val max_dist_ride_match = line.getAs[String]("max_dist_ride_match")
      val total_dist_walk = line.getAs[Double]("total_dist_walk")
      val max_dist_walk = line.getAs[Double]("max_dist_walk")
      val max_dist_walk_match = line.getAs[String]("max_dist_walk_match")
      val online_stages = line.getAs[Int]("online_stages")
      val max_dist_shot = line.getAs[Double]("max_dist_shot")
      val max_dist_shot_match = line.getAs[String]("max_dist_shot_match")
      val max_suvive_time = line.getAs[Double]("max_suvive_time")
      val max_suvive_time_match = line.getAs[String]("max_suvive_time_match")
      val max_kills = line.getAs[Int]("max_kills")
      val max_kills_match = line.getAs[String]("max_kills_match")
      val max_assists = line.getAs[Int]("max_assists")
      val max_assists_match = line.getAs[String]("max_assists_match")
      val count_use_ride = line.getAs[Int]("count_use_ride")
      val kill_death_ratio = line.getAs[Double]("kill_death_ratio")
      val top_10_count = line.getAs[Long]("top_10_count")
      val top_10_ratio = if (play_count > 0) {
        top_10_count / play_count
      } else {
        top_10_count / 1
      }

      GdmPlayerCareerStatsModel(name, first_play_time, first_play_date, last_play_time, total_kills, avg_kills,
        total_assists, avg_assists, total_suvive_time, avg_suvive_time, total_dmg, avg_dmg,
        play_count, win_count, party_count, total_dbno, total_dist_ride, max_dist_ride, max_dist_ride_match,
        total_dist_walk, max_dist_walk, max_dist_walk_match, online_stages, max_dist_shot, max_dist_shot_match,
        max_suvive_time, max_suvive_time_match, max_kills, max_kills_match, max_assists, max_assists_match,
        count_use_ride, kill_death_ratio, top_10_ratio)
    }).write.mode(SaveMode.Overwrite).partitionBy(PARTITION_BY).saveAsTable(targetTableName)
  }

  /**
    * 玩家第一次游戏时间
    *
    * @param spark
    * @param aggWide
    * @return
    */
  def firstPlayTimeDF(spark: SparkSession, aggWide: DataFrame): DataFrame = {
    import spark.implicits._
    val w = Window.partitionBy($"player_name").orderBy(unix_timestamp($"date").asc, unix_timestamp($"time").asc)
    val firstPlayTime = aggWide.withColumn("date_rank", row_number().over(w)).where($"date_rank" === 1)
      .select(
        $"player_name".as("player"),
        $"date".as("first_play_date"),
        concat_ws(" ", $"date", $"time").as("first_play_time")
      )
    firstPlayTime
  }

  /**
    * 玩家最近一次游戏时间
    *
    * @param spark
    * @param aggWide
    * @return
    */
  def lastPlayTimeDF(spark: SparkSession, aggWide: DataFrame): DataFrame = {
    import spark.implicits._
    val w = Window.partitionBy($"player_name").orderBy(unix_timestamp($"date").desc, unix_timestamp($"time").desc)
    val lastPlayTime = aggWide.withColumn("date_rank", row_number().over(w)).where($"date_rank" === 1)
      .select(
        $"player_name".as("player"),
        concat_ws(" ", $"date", $"time").as("last_play_time")
      )
    lastPlayTime
  }

  /**
    * 玩家经常在线时段统计
    *
    * @param spark
    * @param aggWide
    * @return
    */
  def onlineStagesDF(spark: SparkSession, aggWide: DataFrame): DataFrame = {
    import spark.implicits._

    val aggWideGroup = aggWide.groupBy($"player_name", $"hour")
      .agg(count($"hour").as("hour_count"))

    val w = Window.partitionBy($"player_name").orderBy($"hour_count".desc, $"hour".desc)
    val onlineStages = aggWideGroup.withColumn("hour_rank", row_number().over(w)).where($"hour_rank" === 1)
      .select(
        $"player_name".as("player"),
        $"hour".as("online_stages")
      )
    onlineStages
  }

  /**
    * 玩家最远击杀统计
    *
    * @param spark
    * @param killWide
    * @return
    */
  def maxDistShotDF(spark: SparkSession, killWide: DataFrame): DataFrame = {
    import spark.implicits._
    val w = Window.partitionBy($"killer_name").orderBy($"shot_distance".desc)
    val maxDistShot = killWide.filter($"killer_name".isNotNull)
      .withColumn("kl_rank", row_number().over(w)).where($"kl_rank" === 1)
      .select(
        $"match_id".as("max_dist_shot_match"),
        $"killer_name".as("player"),
        $"shot_distance".as("max_dist_shot")
      )
    maxDistShot
  }

  /**
    * 玩家最大生存时间统计
    *
    * @param spark
    * @param aggWide
    * @return
    */
  def maxSuviveTimeDF(spark: SparkSession, aggWide: DataFrame): DataFrame = {
    import spark.implicits._
    val w = Window.partitionBy($"player_name").orderBy($"player_suvive_time".desc)
    val maxSuviveTime = aggWide.withColumn("st_rank", row_number().over(w)).where($"st_rank" === 1)
      .select(
        $"match_id".as("max_suvive_time_match"),
        $"player_name".as("player"),
        $"player_suvive_time".as("max_suvive_time")
      )
    maxSuviveTime
  }

  /**
    * 玩家最大击杀统计
    *
    * @param spark
    * @param aggWide
    * @return
    */
  def maxKillsDF(spark: SparkSession, aggWide: DataFrame): DataFrame = {
    import spark.implicits._
    val w = Window.partitionBy($"player_name").orderBy($"player_kills".desc)
    val maxKills = aggWide.withColumn("pk_rank", row_number().over(w)).where($"pk_rank" === 1)
      .select(
        $"match_id".as("max_kills_match"),
        $"player_name".as("player"),
        $"player_kills".as("max_kills")
      )
    maxKills
  }

  /**
    * 玩家最大助攻统计
    *
    * @param spark
    * @param aggWide
    * @return
    */
  def maxAssistsDF(spark: SparkSession, aggWide: DataFrame): DataFrame = {
    import spark.implicits._
    val w = Window.partitionBy($"player_name").orderBy($"player_assists".desc)
    val maxAssists = aggWide.withColumn("pa_rank", row_number().over(w)).where($"pa_rank" === 1)
      .select(
        $"match_id".as("max_assists_match"),
        $"player_name".as("player"),
        $"player_assists".as("max_assists")
      )
    maxAssists
  }

  /**
    * 玩家TOP10 数字统计
    *
    * @param spark
    * @param aggWide
    * @return
    */
  def top10CountDF(spark: SparkSession, aggWide: DataFrame): DataFrame = {
    import spark.implicits._
    val top10Count = aggWide.where($"team_placement" <= 10).groupBy($"player_name")
      .agg(count("player_name").as("top_10_count"))
      .select($"player_name".as("player"), $"top_10_count")
    top10Count
  }

  /**
    * 玩家使用载具最远统计
    *
    * @param spark
    * @param aggWide
    * @return
    */
  def maxDistRideDF(spark: SparkSession, aggWide: DataFrame): DataFrame = {
    import spark.implicits._
    val w = Window.partitionBy($"player_name").orderBy($"player_dist_ride".desc)
    val maxDistRide = aggWide.withColumn("pdr_rank", row_number().over(w)).where($"pdr_rank" === 1)
      .select(
        $"match_id".as("max_dist_ride_match"),
        $"player_name".as("player"),
        $"player_dist_ride".as("max_dist_ride")
      )
    maxDistRide
  }

  /**
    * 玩家步行最远统计
    *
    * @param spark
    * @param aggWide
    * @return
    */
  def maxDistWalkDF(spark: SparkSession, aggWide: DataFrame): DataFrame = {
    import spark.implicits._
    val w = Window.partitionBy($"player_name").orderBy($"player_dist_walk".desc)
    val maxDistWalk = aggWide.withColumn("pdw_rank", row_number().over(w)).where($"pdw_rank" === 1)
      .select(
        $"match_id".as("max_dist_walk_match"),
        $"player_name".as("player"),
        $"player_dist_walk".as("max_dist_walk")
      )
    maxDistWalk
  }

}
