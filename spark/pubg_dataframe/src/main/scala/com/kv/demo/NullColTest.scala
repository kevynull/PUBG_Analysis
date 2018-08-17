package com.kv.demo

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.collect_set
import org.apache.spark.sql.functions._

object NullColTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("test")
      .master("local[2]")
      .getOrCreate()

    val kill_path = "hdfs://node-0:9000/test_data/kill"

    val kill = spark.read.option("header", "true").option("inferSchema", "true").csv(kill_path)
    import spark.implicits._
    val ks = kill.groupBy("killed_by").agg(collect_set("killer_name").as("killer_list"))
      .select($"killed_by".as("wp"), $"killer_list")

    val killGroup = kill.groupBy("killed_by").agg(count("killer_placement").as("kp"))

    val join = killGroup.join(ks, ks.col("wp") === killGroup.col("killed_by"),"left")

    join.show()

    join.map(line => {
      val killed_by = line.getAs[String]("killed_by")
      val kl = line.getList[String](line.fieldIndex("killer_list")).toArray.mkString(";")
      var killer_list = ""
      /*if (!Option(kl).isEmpty) {
        killer_list = kl.toArray.mkString(";")
      }*/
      Test(killed_by,kl)
    })//.show()
  }

  case class Test(str:String,str1:String)
}
