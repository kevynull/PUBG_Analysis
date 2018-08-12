package com.kv.demo

import org.apache.spark.sql.SparkSession

object ReadJsonAPP {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("read_csv")
      .master("local[2]")
      .getOrCreate()

    val people = spark.read.json("file:///C:/Users/Fren/Desktop/people.json")

    people.show()

    spark.stop()

  }
}
