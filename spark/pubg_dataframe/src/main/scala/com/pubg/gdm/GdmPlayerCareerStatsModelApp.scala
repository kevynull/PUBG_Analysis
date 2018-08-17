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
    val killPageViewTableName = ConfigUtil.DB_NAME + "." + ConfigUtil.GDM_KILL_MATCH_STATS_PAGEVIEW
    val targetTableName = ConfigUtil.DB_NAME + "." + ConfigUtil.GDM_PLAYER_CAREER_STATS_MODEL

    //分区字段
    val PARTITION_BY = "first_play_date"

    /* 必须判断 player_name is not null */
    import spark.implicits._
    val aggWide = spark.table(aggTableName).where($"player_name".isNotNull)
    val killView = spark.table(killPageViewTableName)


  }

  /**
    * 玩家第一次游戏时间
    * @param spark
    * @param aggWide
    * @return
    */
  def firstPlayTimeDF(spark: SparkSession, aggWide: DataFrame): DataFrame = {
    import spark.implicits._
    val w = Window.partitionBy($"player_name")
      .orderBy(
        unix_timestamp(aggWide.col("date")).asc,
        unix_timestamp(aggWide.col("time")).asc
      )
    aggWide.withColumn("date_rank", row_number().over(w)).where($"date_rank" === 1)
      .select($"player_name".as("player"), $"date".as("first_play_date"),
        concat($"date" ,$" " ,$"time").as("first_play_time")
      )
  }

  /**
    * 玩家最近一次游戏时间
    * @param spark
    * @param aggWide
    * @return
    */
  def lastPlayTimeDF(spark: SparkSession, aggWide: DataFrame): DataFrame = {
    import spark.implicits._
    val w = Window.partitionBy($"player_name")
      .orderBy(
        unix_timestamp(aggWide.col("date")).desc,
        unix_timestamp(aggWide.col("time")).desc
      )
    aggWide.withColumn("date_rank", row_number().over(w)).where($"date_rank" === 1)
      .select($"player_name".as("player"), concat($"date" ,$" " ,$"time").as("last_play_time"))
  }

  /**
    * 玩家“吃鸡”游戏次数统计
    * @param spark
    * @param aggWide
    * @return
    */
  def winCountDF(spark: SparkSession, aggWide: DataFrame): DataFrame = {
    import spark.implicits._
    aggWide.where($"team_placement" === 1).groupBy("player_name").agg(count("player_name").as("win_count"))
  }

  /**
    * 玩家组队游戏次数统计
    * @param spark
    * @param aggWide
    * @return
    */
  def partyCountDF(spark: SparkSession, aggWide: DataFrame): DataFrame = {
    aggWide
  }

  /**
    * 玩家经常在线时段统计
    * [需要添加存活游戏时间位移]
    *
    * @param spark
    * @param aggWide
    * @return
    */
  def onlineStagesDF(spark: SparkSession, aggWide: DataFrame): DataFrame = {
    aggWide
  }

  /**
    * 玩家最远击杀统计
    * @param spark
    * @param killView
    * @return
    */
  def maxDistShotDF(spark: SparkSession, killView: DataFrame): DataFrame = {
    killView
  }

  /**
    * 玩家最大生存时间统计
    * @param spark
    * @param aggWide
    * @return
    */
  def maxSuviveTimeDF(spark: SparkSession, aggWide: DataFrame): DataFrame = {
    aggWide
  }

  /**
    * 玩家最大击杀统计
    * @param spark
    * @param aggWide
    * @return
    */
  def maxKillsDF(spark: SparkSession, aggWide: DataFrame): DataFrame = {
    aggWide
  }

  /**
    * 玩家最大助攻统计
    * @param spark
    * @param aggWide
    * @return
    */
  def maxAssistsDF(spark: SparkSession, aggWide: DataFrame): DataFrame = {
    aggWide
  }

  /**
    * 玩家TOP10 概率统计
    * @param spark
    * @param aggWide
    * @return
    */
  def top10RatioDF(spark: SparkSession, aggWide: DataFrame): DataFrame = {
    aggWide
  }

  /**
    * 玩家使用载具最远统计
    * @param spark
    * @param aggWide
    * @return
    */
  def maxDistRideDF(spark: SparkSession, aggWide: DataFrame): DataFrame = {
    aggWide
  }

  /**
    * 玩家步行最远统计
    * @param spark
    * @param aggWide
    * @return
    */
  def maxDistWalkDF(spark: SparkSession, aggWide: DataFrame): DataFrame = {
    aggWide
  }
}
