package com.pubg.base

/**
  * 玩家比赛统计视图表
  *
  * @param date
  * @param time
  * @param pubg_opgg_id
  * @param match_mode
  * @param maximum_distance_shot
  * @param player_assists
  * @param player_dbno
  * @param player_dist_ride
  * @param player_dist_walk
  * @param player_dmg
  * @param player_kills
  * @param player_name
  * @param player_suvive_time
  */
case class GdmPlayerMatchStatsPageview(
   date: String,
   time: String,
   pubg_opgg_id: String,
   match_mode: String,
   maximum_distance_shot: Double,
   player_assists: Int,
   player_dbno: Int,
   player_dist_ride: Double,
   player_dist_walk: Double,
   player_dmg: Int,
   player_kills: Int,
   player_name: String,
   player_suvive_time: Double
 )
