package com.pubg.base

/**
  * 玩家信息，临时表
  *
  * @param player_name
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
  * @param total_dist_walk
  * @param count_use_ride
  * @param kill_death_ratio
  */
case class TempGdmPlayerStats (
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


