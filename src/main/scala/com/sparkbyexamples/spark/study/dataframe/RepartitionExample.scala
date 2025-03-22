package com.sparkbyexamples.spark.study.dataframe

import org.apache.spark.sql.{Dataset, SparkSession}

import java.lang

object RepartitionExample extends App {
  val spark:SparkSession = SparkSession.builder()
    .master("local[5]")
    .appName("SparkByExamples.com")
    //    .config("spark.default.parallelism", "500")
    .getOrCreate()
   spark.sqlContext.setConf("spark.default.parallelism", "500")
  spark.conf.set("spark.default.parallelism", "500")
  private val value: Dataset[lang.Long] = spark.range(0, 20)
  value.printSchema()
  println(value.rdd.partitions.length)

  println(value.repartition(10).rdd.partitions.length)

  println(value.coalesce(2).rdd.partitions.length)

  println(value.groupBy("id").count().rdd.getNumPartitions)
}
