package com.pubg.base

/**
  * 比赛统计模型表
  *
  * @param date
  * @param time
  * @param pubg_opgg_id
  * @param match_time
  * @param match_size
  * @param party_size
  * @param match_mode
  * @param max_kills
  * @param max_kills_name
  * @param max_assists
  * @param max_assists_name
  * @param player_rides_size
  * @param max_dist_ride
  * @param max_dist_ride_name
  * @param max_dist_walk
  * @param max_dist_walk_name
  * @param max_dmg
  * @param max_dmg_name
  * @param maximum_distance_shot
  * @param md_shot_name
  * @param winner_list
  */
case class GdmMatchStatsModel (
  date:String,
  time: String,
  pubg_opgg_id: String,
  match_time: Int,
  match_size: Int,
  party_size: Int,
  player_size: Int,
  match_mode: String,
  max_kills: Int,
  max_kills_name: String,
  max_assists: Int,
  max_assists_name: String,
  player_rides_size: Int,
  max_dist_ride: Double,
  max_dist_ride_name: String,
  max_dist_walk: Double,
  max_dist_walk_name: String,
  max_dmg: Int,
  max_dmg_name: String,
  maximum_distance_shot: Double,
  md_shot_name: String,
  winner_list: String
)

