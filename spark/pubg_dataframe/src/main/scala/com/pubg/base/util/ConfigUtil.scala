package com.pubg.base.util

object ConfigUtil {
  val DB_NAME = "pubg_data"
  val PARTITION = "date"
  val MAP_MAX_POINT = 800000;

  val BDM_AGG_MATCH_STATS = "bdm_agg_match_stats"
  val BDM_KILL_MATCH_STATS = "bdm_kill_match_stats"

  val FDM_AGG_MATCH_WIDE = "fdm_agg_match_wide"
  val FDM_KILL_MATCH_WIDE = "fdm_kill_match_wide"

  val GDM_PLAYER_MATCH_STATS_PAGEVIEW = "gdm_player_match_stats_pageview"
  val GDM_KILL_MATCH_STATS_PAGEVIEW = "gdm_kill_match_stats_pageview"
  val GDM_MATCH_STATS_MODEL = "gdm_match_stats_model"
  val GDM_PLAYER_CAREER_STATS_MODEL = "gdm_player_career_stats_model"



  val AGG_SOURCE_PATHS = "hdfs://node-0:9000/pubg_data/pubg-match-deaths/aggregate"
  val KILL_SOURCE_PATHS = "hdfs://node-0:9000/pubg_data/pubg-match-deaths/deaths"

  val TEST_AGG_SOURCE_PATHS = "hdfs://node-0:9000/test_data/agg"
  val TEST_KILL_SOURCE_PATHS = "hdfs://node-0:9000/test_data/kill"
}
