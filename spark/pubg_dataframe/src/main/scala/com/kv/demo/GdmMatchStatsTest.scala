package com.kv.demo


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object GdmMatchStatsTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("test")
      .master("local[2]")
      .getOrCreate()

    val path = "hdfs://node-0:9000/test_data/agg"

    val agg = spark.read.option("header", "true").option("inferSchema", "true").csv(path)

    /**
      * GdmMatchStatsModelApp 中的查询，其实就是分组取topN的问题，df需要用到开窗函数来处理。
      */
    import spark.implicits._
    val w = Window.partitionBy($"match_id").orderBy($"player_kills".desc)
    val dfTop1 = agg.withColumn("kill_rank", row_number().over(w)).where($"kill_rank" === 1).drop("kill_rank")
    dfTop1.select("match_id","player_name", "player_kills").show()

   /* aggStats.join(maxKill,
      aggStats.col("match_id") === maxKill.col("match_id") ,
      "left")
      .select(agg.col("match_id"),maxKill.col("player_name"),maxKill.col("max_kills")).show()*/

  }
}
