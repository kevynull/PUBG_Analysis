package com.pubg.gdm

import com.pubg.base.{GdmPlayerCareerStatsModel, TempGdmPlayerStats}
import com.pubg.base.util.ConfigUtil
import com.pubg.base.temp._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{row_number, _}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

/**
  *
  */
object GdmPlayerCareerStatsModelAppTask10 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .enableHiveSupport()
      .getOrCreate()

    /* 玩家聚合统计 */
    val task9TableName = ConfigUtil.DB_NAME + "." + ConfigUtil.GDM_PLAYER_TASK_9_TEMP

    /* 玩家 top 1 统计 */
    val maxDistWalkTableName = ConfigUtil.DB_NAME + "." + ConfigUtil.GDM_MAX_DIST_WALK_PLAYER_TEMP

    val targetTableName = ConfigUtil.DB_NAME + "." + ConfigUtil.GDM_PLAYER_CAREER_STATS_MODEL

    //分区字段
    val PARTITION_BY = "first_play_date" //first_play_date

    val task9Stats = spark.table(task9TableName)

    val maxDistWalk = spark.table(maxDistWalkTableName)
    /* 玩家聚合统计 */
    import spark.implicits._
    val gdmPlayerCareerStats = task9Stats
      .join(maxDistWalk, maxDistWalk.col("player") === task9Stats.col("player_name"), "left")

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
      val top_10_ratio = line.getAs[Double]("top_10_ratio")

      GdmPlayerCareerStatsModel(name, first_play_time, first_play_date, last_play_time, total_kills, avg_kills,
        total_assists, avg_assists, total_suvive_time, avg_suvive_time, total_dmg, avg_dmg,
        play_count, win_count, party_count, total_dbno, total_dist_ride, max_dist_ride, max_dist_ride_match,
        total_dist_walk, max_dist_walk, max_dist_walk_match, online_stages, max_dist_shot, max_dist_shot_match,
        max_suvive_time, max_suvive_time_match, max_kills, max_kills_match, max_assists, max_assists_match,
        count_use_ride, kill_death_ratio, top_10_ratio)
    }).write.mode(SaveMode.Overwrite).partitionBy(PARTITION_BY).saveAsTable(targetTableName)
  }
}
