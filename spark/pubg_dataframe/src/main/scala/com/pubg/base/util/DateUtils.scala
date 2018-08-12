package com.pubg.base.util

import java.util.{Date, Locale}

import org.apache.commons.lang3.time.FastDateFormat

/**
 * 日期时间解析工具类:
 * 注意：SimpleDateFormat是线程不安全
 */
object DateUtils {

  //输入文件日期时间格式
  //10/Nov/2016:00:01:02 +0800
  val YYYYMMDDHHMM_TIME_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd'T'HH:mm:ss", Locale.ENGLISH)

  //目标日期格式
  val TARGET_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
  val DATE_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd")
  val TIME_FORMAT = FastDateFormat.getInstance("HH:mm:ss")
  val YEAR_FORMAT = FastDateFormat.getInstance("yyyy")
  val MONTH_FORMAT = FastDateFormat.getInstance("MM")
  val DAY_FORMAT = FastDateFormat.getInstance("dd")

  val HOUR_FORMAT = FastDateFormat.getInstance("HH")
  val MINUTE_FORMAT = FastDateFormat.getInstance("mm")
  val SECONDS_FORMAT = FastDateFormat.getInstance("ss")

  /**
   * 获取时间：yyyy-MM-dd HH:mm:ss
   */
  def parse(time: String) = {
    TARGET_FORMAT.format(new Date(getTime(time)))
  }

  def parse(time: Long) = {
    TARGET_FORMAT.format(new Date(time))
  }

  def parseDate(time: String) ={
    DATE_FORMAT.format(new Date(getTime(time)))
  }

  def parseDate(time: Long) ={
    DATE_FORMAT.format(new Date(time))
  }

  def parseTime(time: String) ={
    TIME_FORMAT.format(new Date(getTime(time)))
  }

  def parseTime(time: Long) ={
    TIME_FORMAT.format(new Date(time))
  }

  def parseYear(time: String) ={
    YEAR_FORMAT.format(new Date(getTime(time)))
  }

  def parseYear(time: Long) ={
    YEAR_FORMAT.format(new Date(time))
  }


  def parseMonth(time: String) ={
    MONTH_FORMAT.format(new Date(getTime(time)))
  }

  def parseMonth(time: Long) ={
    MONTH_FORMAT.format(new Date(time))
  }

  def parseDay(time: String) ={
    DAY_FORMAT.format(new Date(getTime(time)))
  }

  def parseDay(time: Long) ={
    DAY_FORMAT.format(new Date(time))
  }

  def parseHour(time: String) ={
    HOUR_FORMAT.format(new Date(getTime(time)))
  }

  def parseHour(time: Long) ={
    HOUR_FORMAT.format(new Date(time))
  }

  def parseMinute(time: String) ={
    MINUTE_FORMAT.format(new Date(getTime(time)))
  }

  def parseMinute(time: Long) ={
    MINUTE_FORMAT.format(new Date(time))
  }

  def parseSeconds(time: String) ={
    SECONDS_FORMAT.format(new Date(getTime(time)))
  }

  def parseSeconds(time: Long) ={
    SECONDS_FORMAT.format(new Date(time))
  }



  /**
   * 获取输入日志时间：long类型
   *
   * time: [10/Nov/2016:00:01:02 +0800]
   */
  def getTime(time: String) = {
    try {
      YYYYMMDDHHMM_TIME_FORMAT.parse(time).getTime
    } catch {
      case e: Exception => {
        0l
      }
    }
  }

  def main(args: Array[String]) {
    val time = getTime("2017-10-31T02:41:53+0000")
    println(parse(time))
    println(parseDate(time))
    println(parseTime(time))
    println(parseYear(time))
    println(parseMonth(time))
    println(parseDay(time))

    println(parseHour(time))
    println(parseMinute(time))
    println(parseSeconds(time))
  }

}
