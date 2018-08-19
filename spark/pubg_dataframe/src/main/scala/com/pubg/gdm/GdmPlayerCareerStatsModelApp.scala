package com.pubg.gdm

import com.pubg.base.util.ConfigUtil
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.row_number
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

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
    import spark.implicits._
    val aggWide = spark.table(aggTableName).where($"player_name".isNotNull)
    val killWide = spark.table(killTableName)

    val firstPlayTime = firstPlayTimeDF(spark, aggWide)
    val lastPlayTime = lastPlayTimeDF(spark, aggWide)
    val onlineStages = onlineStagesDF(spark, aggWide)
    val maxDistShot = maxDistShotDF(spark, killWide)
    val maxSuviveTime = maxSuviveTimeDF(spark, aggWide)
    val maxKills = maxKillsDF(spark, aggWide)
    val maxAssists = maxAssistsDF(spark, aggWide)
    val top10Ratio = top10RatioDF(spark, aggWide)
    val maxDistRide = maxDistRideDF(spark, aggWide)
    val maxDistWalk = maxDistWalkDF(spark, aggWide)

    /* 玩家聚合统计 */
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
        count("player_name").as("player_count"),
        sum("is_win").as("win_count"),
        sum("is_team").as("party_count"),
        sum("player_dbno").as("total_dbno"),
        sum("player_dist_ride").as("total_dist_ride"),
        sum("player_dist_walk").as("avg_dist_walk"),
        sum("is_use_ride").as("count_use_ride"),
        avg(sum("player_kills") + (sum("player_assists") * 0.3)).as("kill_death_ratio")
      )

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
    aggWide.withColumn("date_rank", row_number().over(w)).where($"date_rank" === 1)
      .select(
        $"player_name".as("player"),
        $"date".as("first_play_date"),
        concat($"date", $" ", $"time").as("first_play_time")
      )
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
    aggWide.withColumn("date_rank", row_number().over(w)).where($"date_rank" === 1)
      .select(
        $"player_name".as("player"),
        concat($"date", $" ", $"time").as("last_play_time")
      )
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
    val w = Window.partitionBy($"player_name").orderBy(count($"hour").desc, $"hour".desc)
    aggWide.withColumn("hour_rank", row_number().over(w)).where($"hour_rank" === 1)
      .select(
        $"player_name".as("player"),
        $"hour".as("online_stages")
      )
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
    killWide.filter($"killer_name".isNotNull)
      .withColumn("kl_rank", row_number().over(w)).where($"kl_rank" === 1)
      .select(
        $"match_id".as("max_dist_shot_match"),
        $"killer_name".as("player"),
        $"shot_distance".as("max_dist_shot")
      )
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
    aggWide.withColumn("st_rank", row_number().over(w)).where($"st_rank" === 1)
      .select(
        $"match_id".as("max_suvive_time_match"),
        $"player_name".as("player"),
        $"player_suvive_time".as("max_suvive_time")
      )
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
    aggWide.withColumn("pk_rank", row_number().over(w)).where($"pk_rank" === 1)
      .select(
        $"match_id".as("max_kills_match"),
        $"player_name".as("player"),
        $"player_kills".as("max_kills")
      )
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
    aggWide.withColumn("pa_rank", row_number().over(w)).where($"pa_rank" === 1)
      .select(
        $"match_id".as("max_assists_match"),
        $"player_name".as("player"),
        $"player_assists".as("max_assists")
      )
  }

  /**
    * 玩家TOP10 概率统计
    *
    * @param spark
    * @param aggWide
    * @return
    */
  def top10RatioDF(spark: SparkSession, aggWide: DataFrame): DataFrame = {
    import spark.implicits._
    aggWide.where($"team_placement" <= 10).groupBy($"player_name")
      .agg(count("player_name").as("top_10_count"))
      .select($"player_name".as("player"), $"top_10_count")
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
    aggWide.withColumn("pdr_rank", row_number().over(w)).where($"pdr_rank" === 1)
      .select(
        $"match_id".as("max_dist_ride_match"),
        $"player_name".as("player"),
        $"player_dist_ride".as("max_dist_ride")
      )
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
    aggWide.withColumn("pdw_rank", row_number().over(w)).where($"pdw_rank" === 1)
      .select(
        $"match_id".as("max_dist_walk_match"),
        $"player_name".as("player"),
        $"player_dist_walk".as("max_dist_walk")
      )
  }
}
