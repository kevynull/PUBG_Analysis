package com.pubg.base

/**
  * 比赛聚合表
  * @param date
  * @param game_size
  * @param match_id
  * @param match_mode
  * @param party_size
  * @param player_assists
  * @param player_dbno
  * @param player_dist_ride
  * @param player_dist_walk
  * @param player_dmg
  * @param player_kills
  * @param player_name
  * @param player_survive_time
  * @param team_id
  * @param team_placement
  */
case class BdmAggMatchStats (
  date:String,
  game_size:Int,
  match_id:String,
  match_mode:String,
  party_size:Int,
  player_assists:Int,
  player_dbno:Int,
  player_dist_ride:Double,
  player_dist_walk:Double,
  player_dmg:Int,
  player_kills:Int,
  player_name:String,
  player_survive_time:Double,
  team_id:Int,
  team_placement:Int
)
