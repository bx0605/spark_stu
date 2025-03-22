package com.sparkbyexamples.spark.study.dataframe.aggrate

import org.apache.spark.sql.{Dataset, Row, SparkSession}

object SQLDistinct extends App {
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
  df.show(false)
  private val value: Dataset[Row] = df.distinct()
value.show(false)
  private val value1: Dataset[Row] = df.dropDuplicates()
  value1.show(false)
  private val value2: Dataset[Row] = df.dropDuplicates("department", "salary")
  value2.show(false)
}
