package com.pubg.base

/**
  * 玩家生涯统计模型表
  *
  * @param name
  * @param first_play_date
  * @param first_play_time
  * @param last_play_time
  * @param total_kills
  * @param avg_kills
  * @param total_assists
  * @param avg_assists
  * @param total_suvive_time
  * @param avg_suvive_time
  * @param total_dmg
  * @param avg_dmg
  * @param play_count
  * @param win_count
  * @param party_count
  * @param total_dbno
  * @param total_dist_ride
  * @param max_dist_ride
  * @param max_dist_ride_match
  * @param total_dist_walk
  * @param max_dist_walk
  * @param max_dist_walk_match
  * @param online_stages_start
  * @param online_stages_end
  * @param max_dist_shot
  * @param max_dist_shot_match
  * @param max_suvive_time
  * @param max_suvive_time_match
  * @param max_kills
  * @param max_kills_match
  * @param max_assists
  * @param max_assists_match
  * @param count_use_ride
  * @param kill_death_ratio
  */
case class GdmPlayerCareerStatsModel (
  name: String,
  first_play_date: String,
  first_play_time: String,
  last_play_time: String,
  total_kills: Int,
  avg_kills: Double,
  total_assists: Int,
  avg_assists: Double,
  total_suvive_time: Int,
  avg_suvive_time: Double,
  total_dmg: Int,
  avg_dmg: Double,
  play_count: Int,
  win_count: Int,
  party_count: Int,
  total_dbno: Int,
  total_dist_ride: Double,
  max_dist_ride: Double,
  max_dist_ride_match: String,
  total_dist_walk: Double,
  max_dist_walk: Double,
  max_dist_walk_match: String,
  online_stages_start: Int,
  online_stages_end: Int,
  max_dist_shot: Double,
  max_dist_shot_match: String,
  max_suvive_time: Double,
  max_suvive_time_match: String,
  max_kills: Int,
  max_kills_match: String,
  max_assists: Int,
  max_assists_match: String,
  count_use_ride: Int,
  kill_death_ratio: Double
)