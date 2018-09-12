package com.pubg.gdm

import com.pubg.base.{GdmPlayerCareerStatsModel, TempGdmPlayerStats}
import com.pubg.base.util.ConfigUtil
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{row_number, _}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

/**
  *
  */
object GdmPlayerCareerStatsModelAppTask4 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .enableHiveSupport()
      .getOrCreate()

    /*  */
    val task3TableName = ConfigUtil.DB_NAME + "." + ConfigUtil.GDM_PLAYER_TASK_3_TEMP

    /* 玩家 top 1 统计 */
    val maxDistShotTableName = ConfigUtil.DB_NAME + "." + ConfigUtil.GDM_MAX_DIST_SHOT_PLAYER_TEMP

    val targetTableName = ConfigUtil.DB_NAME + "." + ConfigUtil.GDM_PLAYER_TASK_4_TEMP

    //分区字段
    val PARTITION_BY = "first_play_date" //first_play_date

    val task3Stats = spark.table(task3TableName)

    val maxDistShot = spark.table(maxDistShotTableName)
  
    /* 玩家聚合统计 */
    import spark.implicits._
    val task4Stats = task3Stats
      .join(maxDistShot, maxDistShot.col("player") === task3Stats.col("player_name"), "left")
      
    task4Stats.map(line => {
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
      val count_use_ride = line.getAs[Int]("count_use_ride")
      val kill_death_ratio = line.getAs[Double]("kill_death_ratio")
      val online_stages = line.getAs[Int]("online_stages")
      val max_dist_shot = line.getAs[Double]("max_dist_shot")
      val max_dist_shot_match = line.getAs[String]("max_dist_shot_match")

      TempGdmPlayerTask4(first_play_date, first_play_time, last_play_time, name, total_kills,
       avg_kills, total_assists, avg_assists, total_suvive_time, avg_suvive_time, 
       total_dmg, avg_dmg, play_count, win_count, party_count, total_dbno,
       total_dist_ride, total_dist_walk, count_use_ride, online_stages, kill_death_ratio,
       max_dist_shot_match, max_dist_shot
      )
    }).write.mode(SaveMode.Overwrite).partitionBy(PARTITION_BY).saveAsTable(targetTableName)
  }
}
