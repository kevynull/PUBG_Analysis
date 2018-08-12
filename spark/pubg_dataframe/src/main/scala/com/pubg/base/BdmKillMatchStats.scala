package com.pubg.base

/**
  * 比赛击杀明细表
  * @param killed_by
  * @param killer_name
  * @param killer_placement
  * @param killer_position_x
  * @param killer_position_y
  * @param map
  * @param match_id
  * @param time
  * @param victim_name
  * @param victim_placement
  * @param victim_position_x
  * @param victim_position_y
  */
case class BdmKillMatchStats (
  killed_by:String,
  killer_name:String,
  killer_placement:Double,
  killer_position_x:Double,
  killer_position_y:Double,
  map:String,
  match_id:String,
  time:Int,
  victim_name:String,
  victim_placement:Double,
  victim_position_x:Double,
  victim_position_y:Double
)
