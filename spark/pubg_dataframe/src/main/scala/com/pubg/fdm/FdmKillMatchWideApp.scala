package com.pubg.fdm

import com.pubg.base.{BdmKillMatchStats, FdmKillMatchWide}
import com.pubg.base.util.{ConfigUtil, PositionUtils}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.count

/**
  * 比赛击杀信息明细宽表
  */
object FdmKillMatchWideApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .enableHiveSupport()
      .getOrCreate()

    val aggTableName = ConfigUtil.DB_NAME + "." + ConfigUtil.BDM_AGG_MATCH_STATS
    val killTableName = ConfigUtil.DB_NAME + "." + ConfigUtil.BDM_KILL_MATCH_STATS
    val targetTableName = ConfigUtil.DB_NAME + "." + ConfigUtil.FDM_KILL_MATCH_WIDE

    val agg = spark.table(aggTableName)
    val kill = spark.table(killTableName)

    val aggGroup = agg.select(agg.col("date"), agg.col("match_id"))
      .groupBy("date", "match_id").agg(count("match_id").as("match_count"))

    import spark.implicits._
    val kill_ds = kill.as[BdmKillMatchStats]

    val agg_kill_join = kill_ds.join(aggGroup, kill_ds.col("match_id") === aggGroup.col("match_id"), "left")
      .where(aggGroup.col("match_id").isNotNull)
    // 有些数据为空，合并之后，有些比赛数据并没有明细击杀记录

    val kill_join = agg_kill_join.select(
      aggGroup.col("date"),
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
    kill_join.map(line => {
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
      val shot_distance = PositionUtils.distance(killer_position_x, victim_position_x,
        killer_position_y, victim_position_y)
      FdmKillMatchWide(date, killed_by, killer_name, killer_placement, killer_position_x, killer_position_y,
        map, match_id, times, victim_name, victim_placement, victim_position_x, victim_position_y,shot_distance)
    }).write.mode(SaveMode.Overwrite).partitionBy(ConfigUtil.PARTITION).saveAsTable(targetTableName)
  }
}
