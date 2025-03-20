package com.sparkbyexamples.spark.study.dataframe
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object CacheFrame {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName(this.getClass.getSimpleName).getOrCreate()
    val reader = spark.read.options(Map("inferSchema" -> "true", "delimiter" -> ",", "header" -> "true")).csv("src/main/resources/zipcodes.csv")
    val reader1 = reader.where(col("State")==="PR").cache()
reader1.show(false)
    val stringToInt = Map.empty[String, Int]
    val m=Map("sdf"->"sdfa")
    println(m)
  }
}
