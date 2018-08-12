package com.pubg.base

/**
  *
  * @param date
  * @param killed_by
  * @param killer_name
  * @param killer_placement
  * @param killer_position_x
  * @param killer_position_y
  * @param map
  * @param match_id
  * @param times
  * @param victim_name
  * @param victim_placement
  * @param victim_position_x
  * @param victim_position_y
  */
case class FdmKillMatchWide (
  date:String,
  killed_by:String,
  killer_name:String,
  killer_placement:Int,
  killer_position_x:Double,
  killer_position_y:Double,
  map:String,
  match_id:String,
  times:Int,
  victim_name:String,
  victim_placement:Int,
  victim_position_x:Double,
  victim_position_y:Double
)
