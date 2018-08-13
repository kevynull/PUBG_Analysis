package com.pubg.base.util

/**
  * 坐标工具
  */
object PositionUtils {

  /**
    * 转换比例
    * @param ratio
    * @param value
    * @return
    */
  def coordinateScale(ratio: Double, value: Double): Double = {
    value / ConfigUtil.MAP_MAX_POINT * ratio
  }

  /**
    * 计算2点之间的距离
    *
    * @param x1
    * @param x2
    * @param y1
    * @param y2
    * @return
    */
  def distance(x1: Double, x2: Double, y1: Double, y2: Double): Double = {
    val xSqr = (x1 - x2) * (x1 - x2)
    val ySqr = (y1 - y2) * (y1 - y2)
    math.sqrt(xSqr + ySqr)
  }

  def main(args: Array[String]): Unit = {
    val r = coordinateScale(600, 529007)
    println(r)
  }

}
