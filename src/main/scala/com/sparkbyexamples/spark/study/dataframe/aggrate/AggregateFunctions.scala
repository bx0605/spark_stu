package com.sparkbyexamples.spark.study.dataframe.aggrate

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, collect_list, collect_set, first, last, max, mean, min}

object AggregateFunctions extends App {
  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  val simpleData = Seq(("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4100),
    ("Maria", "Finance", 3000),
    ("James", "Sales", 3000),
    ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kumar", "Marketing", 2000),
    ("Saif", "Sales", 4100)
  )
  val df = simpleData.toDF("employee_name", "department", "salary")

  df.select(avg("salary")).show(false)
  df.select(collect_list("salary")).show(false)
  df.select(collect_set("salary")).show(false)
  df.select(first("salary")).show(false)
  df.select(last("salary")).show(false)
  df.select(max("salary")).show(false)
  df.select(min("salary")).show(false)
  df.select(mean("salary")).show(false)
}
