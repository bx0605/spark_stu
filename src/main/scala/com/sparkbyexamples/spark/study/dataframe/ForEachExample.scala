package com.sparkbyexamples.spark.study.dataframe

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.util.LongAccumulator

object ForEachExample extends App {
  private val spark: SparkSession = SparkSession.builder().master("local[1]").appName(this.getClass.getSimpleName).getOrCreate()
  val data = Seq(("Banana",1000,"USA"), ("Carrots",1500,"USA"), ("Beans",1600,"USA"),
    ("Orange",2000,"USA"),("Orange",2000,"USA"),("Banana",400,"China"),
    ("Carrots",1200,"China"),("Beans",1500,"China"))

  private val df: DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(data)).toDF("Product", "Amount", "Country")
  df.foreach(d=>{
    println(d)
  })

  private val accumulator: LongAccumulator = spark.sparkContext.longAccumulator
  df.foreach(d=>{
    accumulator.add(d.getInt(1))
  })
println(accumulator.value)

  private val accumulator1: LongAccumulator = spark.sparkContext.longAccumulator

  val rdd3 = spark.sparkContext.parallelize(Seq(1, 2, 3, 4, 5, 6, 7))
  rdd3.foreach(x=>{
    accumulator1.add(x)
  })
  println(accumulator1.value)
}
