package com.pubg.gdm

import com.pubg.base.util.ConfigUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  *
  */
object GdmMatchStatsModelApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .enableHiveSupport()
      .getOrCreate()

    val aggTableName = ConfigUtil.DB_NAME + "." + ConfigUtil.FDM_AGG_MATCH_WIDE
    val killPageViewTableName = ConfigUtil.DB_NAME + "." + ConfigUtil.GDM_KILL_MATCH_STATS_PAGEVIEW
    val targetTableName = ConfigUtil.DB_NAME + "." + ConfigUtil.GDM_MATCH_STATS_MODEL

    val aggWide = spark.table(aggTableName)
    val killView = spark.table(killPageViewTableName)

    /**
      * select
      * match_id,date,time,game_size,match_mode,max(player_kills), max(player_assists),
      * max(player_dbno),max(player_dmg),count(is_use_ride)
      * from fdm_agg_match_wide
      * group by match_id,date,time,game_size,match_mode limit 50
      */
    import spark.implicits._
    /*val maxKills = aggWide.select().groupBy()
      .agg(max("player_kills"),max)*/
  }
}
