package com.pubg.gdm

import com.pubg.base.util.ConfigUtil
import com.pubg.base._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{row_number, _}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

/**
  * 
  */
object BeforeGdmPlayerMaxStats {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .enableHiveSupport()
      .getOrCreate()

    val aggTableName = ConfigUtil.DB_NAME + "." + ConfigUtil.FDM_AGG_MATCH_WIDE
    val killTableName = ConfigUtil.DB_NAME + "." + ConfigUtil.FDM_KILL_MATCH_WIDE
    val firstPlayTimeTableName = ConfigUtil.DB_NAME + "." + ConfigUtil.GDM_FIRST_PLAY_TIME_PLAYER_TEMP
    val lastPlayTimeTableName = ConfigUtil.DB_NAME + "." + ConfigUtil.GDM_LAST_PLAY_TIME_PLAYER_TEMP
    val onlineStagesTableName = ConfigUtil.DB_NAME + "." + ConfigUtil.GDM_ONLINE_STAGES_PLAYER_TEMP
    val maxDistShotTableName = ConfigUtil.DB_NAME + "." + ConfigUtil.GDM_MAX_DIST_SHOT_PLAYER_TEMP
    val maxSuviveTimeTableName = ConfigUtil.DB_NAME + "." + ConfigUtil.GDM_MAX_SUVIVE_TIME_PLAYER_TEMP
    val maxKillsTableName = ConfigUtil.DB_NAME + "." + ConfigUtil.GDM_MAX_KILLS_PLAYER_TEMP
    val maxAssistsTableName = ConfigUtil.DB_NAME + "." + ConfigUtil.GDM_MAX_ASSISTS_PLAYER_TEMP
    val top10CountTableName = ConfigUtil.DB_NAME + "." + ConfigUtil.GDM_TOP_10_COUNT_PLAYER_TEMP
    val maxDistRideTableName = ConfigUtil.DB_NAME + "." + ConfigUtil.GDM_MAX_DIST_RIDE_PLAYER_TEMP
    val maxDistWalkTableName = ConfigUtil.DB_NAME + "." + ConfigUtil.GDM_MAX_DIST_WALK_PLAYER_TEMP


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
    firstPlayTime.write.mode(SaveMode.Overwrite).saveAsTable(firstPlayTimeTableName)
    lastPlayTime.write.mode(SaveMode.Overwrite).saveAsTable(lastPlayTimeTableName)
    onlineStages.write.mode(SaveMode.Overwrite).saveAsTable(onlineStagesTableName)
    maxDistShot.write.mode(SaveMode.Overwrite).saveAsTable(maxDistShotTableName)
    maxSuviveTime.write.mode(SaveMode.Overwrite).saveAsTable(maxSuviveTimeTableName)
    maxKills.write.mode(SaveMode.Overwrite).saveAsTable(maxKillsTableName)
    maxAssists.write.mode(SaveMode.Overwrite).saveAsTable(maxAssistsTableName)
    top10Count.write.mode(SaveMode.Overwrite).saveAsTable(top10CountTableName)
    maxDistRide.write.mode(SaveMode.Overwrite).saveAsTable(maxDistRideTableName)
    maxDistWalk.write.mode(SaveMode.Overwrite).saveAsTable(maxDistWalkTableName)

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
    firstPlayTime.map(line => {
      val player = line.getAs[String]("player")
      val first_play_date = line.getAs[String]("first_play_date")
      val first_play_time = line.getAs[String]("first_play_time")

      TempGdmFirstPlayTime(player, first_play_date, first_play_time)
    })
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
    lastPlayTime.map(line => {
      val player = line.getAs[String]("player")
      val last_play_time = line.getAs[String]("last_play_time")

      TempGdmLastPlayTime(player, last_play_time)
    })
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
    onlineStages.map(line => {
      val player = line.getAs[String]("player")
      val online_stages = line.getAs[Int]("online_stages")

      TempGdmOnlineStages(player, online_stages)
    })
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
    maxDistShot.map(line => {
      val player = line.getAs[String]("player")
      val max_dist_shot_match = line.getAs[String]("max_dist_shot_match")
      val max_dist_shot = line.getAs[Double]("max_dist_shot")

      TempGdmMaxDistShot(player, max_dist_shot_match, max_dist_shot)
    })
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
    maxSuviveTime.map(line => {
      val player = line.getAs[String]("player")
      val max_suvive_time_match = line.getAs[String]("max_suvive_time_match")
      val max_suvive_time = line.getAs[Double]("max_suvive_time")

      TempGdmMaxSuviveTime(player, max_suvive_time_match, max_suvive_time)
    })
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
    maxKills.map(line => {
      val player = line.getAs[String]("player")
      val max_kills_match = line.getAs[String]("max_kills_match")
      val max_kills = line.getAs[Int]("max_kills")

      TempGdmMaxKills(player, max_kills_match, max_kills)
    })
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
    maxAssists.map(line => {
      val player = line.getAs[String]("player")
      val max_assists_match = line.getAs[String]("max_assists_match")
      val max_assists = line.getAs[Int]("max_assists")

      TempGdmMaxAssists(player, max_assists_match, max_assists)
    })
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
    top10Count.map(line => {
      val player = line.getAs[String]("player")
      val top_10_count = line.getAs[Int]("top_10_count")

      TempGdmTop10Count(player, top_10_count)
    })
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
    maxDistRide.map(line => {
      val player = line.getAs[String]("player")
      val max_dist_ride_match = line.getAs[String]("max_dist_ride_match")
      val max_dist_ride = line.getAs[Double]("max_dist_ride")

      TempGdmMaxDistRide(player, max_dist_ride_match, max_dist_ride)
    })
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
    maxDistWalk.map(line => {
      val player = line.getAs[String]("player")
      val max_dist_walk_match = line.getAs[String]("max_dist_walk_match")
      val max_dist_walk = line.getAs[Double]("max_dist_walk")

      TempGdmMaxDistWalk(player, max_dist_walk_match, max_dist_walk)
    })
  }
}