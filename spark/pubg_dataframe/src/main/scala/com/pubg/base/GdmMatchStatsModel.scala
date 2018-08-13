package com.pubg.base

/**
  * 比赛统计模型表
  * @param date
  * @param time
  * @param match_time
  * @param map
  * @param match_size
  * @param match_mode
  * @param max_kills
  * @param player_rides_size
  * @param max_dist_ride
  * @param max_dist_walk
  * @param max_dmg
  * @param maximum_distance_shot
  * @param md_shot_name
  * @param winner_list
  */
case class GdmMatchStatsModel (
  date:String,
  time: String,
  match_time: Int,
  map: String,
  match_size: Int,
  match_mode: String,
  max_kills: Int,
  player_rides_size: Int,
  max_dist_ride: Double,
  max_dist_walk: Double,
  max_dmg: Int,
  maximum_distance_shot: Double,
  md_shot_name: String,
  winner_list: String
)

