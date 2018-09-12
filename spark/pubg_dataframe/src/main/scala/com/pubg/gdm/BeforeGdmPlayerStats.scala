package com.pubg.gdm

import com.pubg.base.GdmPlayerCareerStatsModel
import com.pubg.base.util.ConfigUtil
import com.pubg.base.temp._
import org.apache.spark.sql.functions.{row_number, _}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

/**
  * 玩家聚合
  */
object BeforeGdmPlayerStats {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .enableHiveSupport()
      .getOrCreate()

    val aggTableName = ConfigUtil.DB_NAME + "." + ConfigUtil.FDM_AGG_MATCH_WIDE
    val targetTableName = ConfigUtil.DB_NAME + "." + ConfigUtil.GDM_PLAYER_STATS_TEMP

    val aggWide = spark.table(aggTableName)

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
    }).coalesce(1).write.mode(SaveMode.Overwrite).saveAsTable(targetTableName)
  }
}