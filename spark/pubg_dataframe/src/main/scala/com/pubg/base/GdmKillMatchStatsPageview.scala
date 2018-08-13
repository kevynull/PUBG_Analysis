package com.pubg.base

/**
  * 比赛击杀信息明细视图表
  * @param date
  * @param match_id
  * @param pubg_opgg_id
  * @param times
  * @param killed_by
  * @param killer_name
  * @param killer_placement
  * @param killer_position_x
  * @param killer_position_y
  * @param killer_position_600_x
  * @param killer_position_600_y
  * @param killer_position_800_x
  * @param killer_position_800_y
  * @param map
  * @param victim_name
  * @param victim_placement
  * @param victim_position_x
  * @param victim_position_y
  * @param victim_position_600_x
  * @param victim_position_600_y
  * @param victim_position_800_x
  * @param victim_position_800_y
  * @param shot_distance
  */
case class GdmKillMatchStatsPageview (
  date:String,
  pubg_opgg_id:String,
  times:Int,
  killed_by:String,
  killer_name:String,
  killer_placement:Int,
  killer_position_x:Double,
  killer_position_y:Double,
  killer_position_600_x:Double,
  killer_position_600_y:Double,
  killer_position_800_x:Double,
  killer_position_800_y:Double,
  map:String,
  victim_name:String,
  victim_placement:Int,
  victim_position_x:Double,
  victim_position_y:Double,
  victim_position_600_x:Double,
  victim_position_600_y:Double,
  victim_position_800_x:Double,
  victim_position_800_y:Double,
  shot_distance:Double
)
