package com.sparkbyexamples.spark.study.dataframe.collection

import org.apache.spark.sql.functions.{array_contains, col}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}

object ArrayContainsExample extends App {
  val spark = SparkSession.builder().appName("SparkByExamples.com")
    .master("local[1]")
    .getOrCreate()

  val data = Seq(
    Row("James,,Smith",List("Java","Scala","C++"),"CA"),
    Row("Michael,Rose,",List("Spark","Java","C++"),"NJ"),
    Row("Robert,,Williams",null,"NV")
  )

  val schema = new StructType()
    .add("name",StringType)
    .add("languagesAtSchool", ArrayType(StringType))
    .add("currentState", StringType)

  val df = spark.createDataFrame(
    spark.sparkContext.parallelize(data),schema)
  df.printSchema()
  df.show(false)

  df.withColumn("java present",array_contains(col("languagesAtSchool"),"java")).show(false)
  df.where(array_contains(col("languagesAtSchool"),"Spark")).show(false)
}
