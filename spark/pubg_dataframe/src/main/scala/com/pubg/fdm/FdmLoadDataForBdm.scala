package com.pubg.fdm

import com.pubg.base.util.{ConfigUtil, DateUtils}
import com.pubg.base.{BdmAggMatchStats, BdmKillMatchStats, FdmAggMatchWide, FdmKillMatchWide}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * 基础数据层，将BDM层中的数据进行扩宽，以及格式处理
  */
object FdmLoadDataForBdm {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .enableHiveSupport()
      .getOrCreate()

    /* local */
    /*val spark = SparkSession.builder()
      .appName("read_csv")
      .master("local[2]")
      .getOrCreate()*/

    val aggGroupDf = statsFdmAggMatchWide(spark)

    statsFdmKillMatchWide(spark, aggGroupDf)


    spark.stop()
  }

  /**
    * 比赛信息聚合统计宽表
    * @param spark
    * @return
    */
  def statsFdmAggMatchWide(spark: SparkSession): DataFrame = {
    val sourceTableName = ConfigUtil.DB_NAME + "." + ConfigUtil.BDM_AGG_MATCH_STATS
    val targetTableName = ConfigUtil.DB_NAME + "." + ConfigUtil.FDM_AGG_MATCH_WIDE

    val agg = spark.table(sourceTableName)

    import spark.implicits._
    val agg_ds = agg.as[BdmAggMatchStats]

    val agg_wide = agg_ds.map(line => {
      val timestamp = DateUtils.getTime(line.date)
      val date = DateUtils.parseDate(timestamp)
      val time = DateUtils.parseTime(timestamp)
      val year = DateUtils.parseYear(timestamp).toInt
      val month = DateUtils.parseMonth(timestamp).toInt
      val day = DateUtils.parseDay(timestamp).toInt
      val hour = DateUtils.parseHour(timestamp).toInt
      val minute = DateUtils.parseMinute(timestamp).toInt
      val seconds = DateUtils.parseSeconds(timestamp).toInt
      FdmAggMatchWide(date, time, year, month, day, hour, minute, seconds, line.game_size, line.match_id,
        line.match_mode, line.party_size, line.player_assists, line.player_dbno, line.player_dist_ride,
        line.player_dist_walk, line.player_dmg, line.player_kills, line.player_name, line.player_survive_time,
        line.team_id, line.team_placement
      )
    })
    agg_wide.write.mode(SaveMode.Overwrite).partitionBy(ConfigUtil.PARTITION).saveAsTable(targetTableName)

    val agg_group = agg_wide.select(agg_wide.col("date"), agg_wide.col("match_id"))
      .groupBy("date","match_id").agg(count("match_id").as("match_count"))
    agg_group
  }

  /**
    * 击杀记录统计宽表
    *
    * select count(1) from bdm_kill_match_stats kill
    * left join(select date,match_id from fdm_agg_match_wide group by date,match_id) agg_group
    * on kill.match_id = agg_group.match_id where agg_group.match_id is not null;
    *
    * @param spark
    * @param agg_group
    * @return
    */
  def statsFdmKillMatchWide(spark: SparkSession, agg_group: DataFrame): Unit = {
    val tableName = ConfigUtil.DB_NAME + "." + ConfigUtil.BDM_KILL_MATCH_STATS
    val targetTableName = ConfigUtil.DB_NAME + "." + ConfigUtil.FDM_KILL_MATCH_WIDE
    val kill = spark.table(tableName)

    import spark.implicits._
    val kill_ds = kill.as[BdmKillMatchStats]

    val agg_kill_join = kill_ds.join(agg_group, kill_ds.col("match_id") === agg_group.col("match_id"),"left")
      .where(agg_group.col("match_id").isNotNull)

    val kill_join = agg_kill_join.select(
      agg_group.col("date"),
      kill_ds.col("killed_by"),
      kill_ds.col("killer_name"),
      kill_ds.col("killer_placement"),
      kill_ds.col("killer_position_x"),
      kill_ds.col("killer_position_y"),
      kill_ds.col("map"),
      kill_ds.col("match_id"),
      kill_ds.col("time"),
      kill_ds.col("victim_name"),
      kill_ds.col("victim_placement"),
      kill_ds.col("victim_position_x"),
      kill_ds.col("victim_position_y")
    )
    val kill_wide = kill_join.map(line => {
      val date = line.getAs[String]("date")
      val killed_by = line.getAs[String]("killed_by")
      val killer_name = line.getAs[String]("killer_name")
      val killer_placement = line.getAs[Double]("killer_placement").toInt
      val killer_position_x = line.getAs[Double]("killer_position_x")
      val killer_position_y = line.getAs[Double]("killer_position_y")
      val map = line.getAs[String]("map")
      val match_id = line.getAs[String]("match_id")
      val times = line.getAs[Int]("time")
      val victim_name = line.getAs[String]("victim_name")
      val victim_placement = line.getAs[Double]("victim_placement").toInt
      val victim_position_x = line.getAs[Double]("victim_position_x")
      val victim_position_y = line.getAs[Double]("victim_position_y")
      FdmKillMatchWide(date, killed_by, killer_name, killer_placement, killer_position_x, killer_position_y,
        map, match_id, times, victim_name, victim_placement, victim_position_x, victim_position_y)
    })
    kill_wide.write.mode(SaveMode.Overwrite).partitionBy(ConfigUtil.PARTITION).saveAsTable(targetTableName)
  }
}
