package com.pubg.gdm

import com.pubg.base.{FdmKillMatchWide, GdmKillMatchStatsPageview}
import com.pubg.base.util.{ConfigUtil, PositionUtils}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 每一局比赛的玩家击杀数据明细
  */
object GdmKillMatchStatsPageviewApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .enableHiveSupport()
      .getOrCreate()

    val killTableName = ConfigUtil.DB_NAME + "." + ConfigUtil.FDM_KILL_MATCH_WIDE
    val targetTableName = ConfigUtil.DB_NAME + "." + ConfigUtil.GDM_KILL_MATCH_STATS_PAGEVIEW

    val killWide = spark.table(killTableName)

    import spark.implicits._
    killWide.as[FdmKillMatchWide].map(line => {
      val date = line.date
      val times = line.times
      val pubg_opgg_id = line.match_id
      val killed_by = line.killed_by
      val killer_name = line.killer_name
      val killer_placement = line.killer_placement
      val killer_position_x = line.killer_position_x
      val killer_position_y = line.killer_position_y
      val killer_position_600_x = PositionUtils.coordinateScale(600, line.killer_position_x)
      val killer_position_600_y = PositionUtils.coordinateScale(600, line.killer_position_y)
      val killer_position_800_x = PositionUtils.coordinateScale(800, line.killer_position_x)
      val killer_position_800_y = PositionUtils.coordinateScale(800, line.killer_position_y)
      val map = line.map
      val victim_name = line.victim_name
      val victim_placement = line.victim_placement
      val victim_position_x = line.victim_position_x
      val victim_position_y = line.victim_position_y
      val victim_position_600_x = PositionUtils.coordinateScale(600, line.victim_position_x)
      val victim_position_600_y = PositionUtils.coordinateScale(600, line.victim_position_y)
      val victim_position_800_x = PositionUtils.coordinateScale(800, line.victim_position_x)
      val victim_position_800_y = PositionUtils.coordinateScale(800, line.victim_position_y)
      val shot_distance = PositionUtils.distance(line.killer_position_x, line.victim_position_x,
        line.killer_position_y, line.victim_position_y)

      GdmKillMatchStatsPageview(date, pubg_opgg_id, times, killed_by, killer_name, killer_placement,
        killer_position_x, killer_position_y, killer_position_600_x, killer_position_600_y,
        killer_position_800_x, killer_position_800_y, map, victim_name, victim_placement, victim_position_x,
        victim_position_y, victim_position_600_x, victim_position_600_y, victim_position_800_x, victim_position_800_y,
        shot_distance)
    }).write.mode(SaveMode.Overwrite).partitionBy(ConfigUtil.PARTITION).saveAsTable(targetTableName)
  }
}
