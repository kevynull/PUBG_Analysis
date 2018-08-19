package com.pubg.gdm

import com.pubg.base.util.{ConfigUtil, PositionUtils}
import com.pubg.base.{FdmKillMatchWide, TempGdmKillDistance, GdmPlayerMatchStatsPageview}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 玩家比赛统计视图表
  *
  * 每一局比赛的每位玩家局内数据（相当于2张基础表的混合统计）
  */
object GdmPlayerMatchStatsPageviewApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .enableHiveSupport()
      .getOrCreate()

    val aggTableName = ConfigUtil.DB_NAME + "." + ConfigUtil.FDM_AGG_MATCH_WIDE
    val killTableName = ConfigUtil.DB_NAME + "." + ConfigUtil.FDM_KILL_MATCH_WIDE
    val targetTableName = ConfigUtil.DB_NAME + "." + ConfigUtil.GDM_PLAYER_MATCH_STATS_PAGEVIEW

    val aggWide = spark.table(aggTableName)
    val killWide = spark.table(killTableName)

    /**
      * -- 统计击杀表射击距离
      * select
      * max(sqrt(((kw.killer_position_x - kw.victim_position_x) * (kw.killer_position_x - kw.victim_position_x)
      * + (kw.killer_position_y - kw.victim_position_y) * (kw.killer_position_y - kw.victim_position_y))))
      * as shot_distance , kw.match_id , kw.killer_name
      * from fdm_kill_match_wide kw where kw.killer_name is not null group by kw.match_id , kw.killer_name
      */

    /* 统计击杀表射击距离 */
    /*val tempKillWide = killWide.select(killWide.col("match_id"), killWide.col("killer_name"))
      .where(killWide.col("killer_name").isNotNull)
      .agg(sqrt(d).as("shot_distance"))*/

    /* 这里的代码需要优化 start com.kv.demo.SqlColTest*/
    import spark.implicits._
    val tempKillWide = killWide.where(killWide.col("killer_name").isNotNull).as[FdmKillMatchWide].map(line => {
      val x1 = line.killer_position_x
      val x2 = line.victim_position_x
      val y1 = line.killer_position_y
      val y2 = line.victim_position_y
      val shot_dis = PositionUtils.distance(x1,x2,y1,y2)
      TempGdmKillDistance(shot_dis,line.match_id,line.killer_name)
    }).select("shot_distance","killer_name","match_id")

    val killShotDis = tempKillWide.groupBy("killer_name","match_id").agg(max("shot_distance").as("shot_distance"))
    /* end */

    val sql_on = killShotDis.col("match_id") === aggWide.col("match_id") && killShotDis.col("killer_name") === aggWide.col("player_name")
    val playerMatchStats = aggWide.join(killShotDis, sql_on, "left")

    playerMatchStats.select(
      aggWide.col("date"),
      aggWide.col("time"),
      aggWide.col("match_id"),
      aggWide.col("match_mode"),
      aggWide.col("party_size"),
      aggWide.col("player_assists"),
      aggWide.col("player_dbno"),
      aggWide.col("player_dist_ride"),
      aggWide.col("player_dist_walk"),
      aggWide.col("player_dmg"),
      aggWide.col("player_kills"),
      aggWide.col("player_name"),
      aggWide.col("player_suvive_time"),
      aggWide.col("team_placement"),
      aggWide.col("is_use_ride"),
      killShotDis.col("shot_distance")
    ).map(line => {
      val date = line.getAs[String]("date")
      val time = line.getAs[String]("time")
      val pubg_opgg_id = line.getAs[String]("match_id")
      val match_mode = line.getAs[String]("match_mode")
      val party_size = line.getAs[Int]("party_size")
      var is_party_team = 0
      if (party_size > 1){
        is_party_team = 1
      }
      val team_placement = line.getAs[Int]("team_placement")
      var is_winner = 0
      if (team_placement == 1){
        is_winner = 1
      }
      val maximum_shot_distance = line.getAs[Double]("shot_distance")
      val player_assists = line.getAs[Int]("player_assists")
      val player_dbno = line.getAs[Int]("player_dbno")
      val player_dist_ride = line.getAs[Double]("player_dist_ride")
      val player_dist_walk = line.getAs[Double]("player_dist_walk")
      val player_dmg = line.getAs[Int]("player_dmg")
      val player_kills = line.getAs[Int]("player_kills")
      val player_name = line.getAs[String]("player_name")
      val player_suvive_time = line.getAs[Double]("player_suvive_time")
      val is_use_ride = line.getAs[Int]("is_use_ride")

      GdmPlayerMatchStatsPageview(date, time, pubg_opgg_id, match_mode, maximum_shot_distance, player_assists,
        player_dbno, player_dist_ride, player_dist_walk, player_dmg, player_kills, player_name, player_suvive_time,
        is_party_team,party_size,is_winner,is_use_ride,team_placement)
    }).write.mode(SaveMode.Overwrite).partitionBy(ConfigUtil.PARTITION).saveAsTable(targetTableName)

  }


}
