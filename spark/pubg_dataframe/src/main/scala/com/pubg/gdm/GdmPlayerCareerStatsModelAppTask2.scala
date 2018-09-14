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
object GdmPlayerCareerStatsModelAppTask2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .enableHiveSupport()
      .getOrCreate()

    /* 任务1 */
    val task1TableName = ConfigUtil.DB_NAME + "." + ConfigUtil.GDM_PLAYER_TASK_1_TEMP

    /* 玩家 top 1 统计 */
    val lastPlayTimeTableName = ConfigUtil.DB_NAME + "." + ConfigUtil.GDM_LAST_PLAY_TIME_PLAYER_TEMP

    val targetTableName = ConfigUtil.DB_NAME + "." + ConfigUtil.GDM_PLAYER_TASK_2_TEMP

    //分区字段
    val PARTITION_BY = "first_play_date" //first_play_date

    val task1Stats = spark.table(task1TableName)

    val lastPlayTime = spark.table(lastPlayTimeTableName)

    /* 玩家聚合统计 */
    import spark.implicits._
    val task2Stats = task1Stats
      .join(lastPlayTime, lastPlayTime.col("player") === task1Stats.col("player_name"), "left")
      
    task2Stats.map(line => {
      val name = line.getAs[String]("player_name")
      val first_play_time = line.getAs[String]("first_play_time")
      val first_play_date = line.getAs[String]("first_play_date")
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
      val total_dist_walk = line.getAs[Double]("total_dist_walk")
      val count_use_ride = line.getAs[Int]("count_use_ride")
      val kill_death_ratio = line.getAs[Double]("kill_death_ratio")

      val last_play_time = line.getAs[String]("last_play_time")

      TempGdmPlayerTask2(first_play_date, first_play_time, last_play_time, name, total_kills,
       avg_kills, total_assists, avg_assists, total_suvive_time, avg_suvive_time, 
       total_dmg, avg_dmg, play_count, win_count, party_count, total_dbno,
       total_dist_ride, total_dist_walk, count_use_ride, kill_death_ratio
      )
    }).write.mode(SaveMode.Overwrite).partitionBy(PARTITION_BY).saveAsTable(targetTableName)
  }
}
