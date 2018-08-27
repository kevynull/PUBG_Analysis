package com.pubg.fdm

import com.pubg.base.{BdmAggMatchStats, FdmAggMatchWide}
import com.pubg.base.util.{ConfigUtil, DateUtils}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 玩家比赛信息统计宽表
  */
object FdmAggMatchWideApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .enableHiveSupport()
      .getOrCreate()

    val sourceTableName = ConfigUtil.DB_NAME + "." + ConfigUtil.BDM_AGG_MATCH_STATS
    val targetTableName = ConfigUtil.DB_NAME + "." + ConfigUtil.FDM_AGG_MATCH_WIDE

    val agg = spark.table(sourceTableName)

    import spark.implicits._
    val aggDS = agg.where($"player_name".isNotNull).as[BdmAggMatchStats] // player_name 为空，不统计

    aggDS.map(line => {
      val timestamp = DateUtils.getTime(line.date)
      val date = DateUtils.parseDate(timestamp)
      val time = DateUtils.parseTime(timestamp)
      val year = DateUtils.parseYear(timestamp).toInt
      val month = DateUtils.parseMonth(timestamp).toInt
      val day = DateUtils.parseDay(timestamp).toInt
      val hour = DateUtils.parseHour(timestamp).toInt
      val minute = DateUtils.parseMinute(timestamp).toInt
      val seconds = DateUtils.parseSeconds(timestamp).toInt
      val isUseRide = if (line.player_dist_ride > 0) {
        1
      } else {
        0
      }
      val isWin = if (line.team_placement == 1) {
        1
      } else {
        0
      }
      val isTeam = if (line.party_size > 1) {
        1
      } else {
        0
      }

      FdmAggMatchWide(date, time, year, month, day, hour, minute, seconds, line.game_size, line.match_id,
        line.match_mode, line.party_size, line.player_assists, line.player_dbno, line.player_dist_ride,
        line.player_dist_walk, line.player_dmg, line.player_kills, line.player_name, line.player_survive_time,
        line.team_id, line.team_placement, isUseRide, isWin, isTeam
      )
    }).coalesce(1).write.mode(SaveMode.Overwrite).partitionBy(ConfigUtil.PARTITION).saveAsTable(targetTableName)
  }
}
