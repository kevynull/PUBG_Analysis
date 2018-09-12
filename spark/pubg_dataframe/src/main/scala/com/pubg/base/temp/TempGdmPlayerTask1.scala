package com.pubg.base.temp

case class TempGdmPlayerTask1(
  first_play_date:String,
  first_play_time:String,
  player_name: String,
  total_kills: Int,
  avg_kills: Double,
  total_assists: Int,
  avg_assists: Double,
  total_suvive_time: Double,
  avg_suvive_time: Double,
  total_dmg: Int,
  avg_dmg: Double,
  play_count: Int,
  win_count: Int,
  party_count: Int,
  total_dbno: Int,
  total_dist_ride: Double,
  total_dist_walk: Double,
  count_use_ride: Int,
  kill_death_ratio: Double
)