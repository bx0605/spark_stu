package com.sparkbyexamples.spark.study.dataframe.collection

import com.sparkbyexamples.spark.dataframe.functions.collection.ArrayOfArrayType.{df, spark}
import org.apache.spark.sql.functions.{explode, flatten}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}

object ArrayOfArrayType extends App {
  val spark = SparkSession.builder().appName("SparkByExamples.com")
    .master("local[1]")
    .getOrCreate()

  val arrayArrayData = Seq(
    Row("James",List(List("Java","Scala","C++"),List("Spark","Java"))),
    Row("Michael",List(List("Spark","Java","C++"),List("Spark","Java"))),
    Row("Robert",List(List("CSharp","VB"),List("Spark","Python")))
  )

  val arrayArraySchema = new StructType().add("name",StringType)
    .add("subjects",ArrayType(ArrayType(StringType)))

  val df = spark.createDataFrame(
    spark.sparkContext.parallelize(arrayArrayData),arrayArraySchema)
  df.printSchema()
  df.show(false)
  import spark.implicits._

  private val frame: DataFrame = df.select($"name", explode($"subjects"))
  frame.printSchema()
  frame
    .show(false)


  df.select($"name",flatten($"subjects")).show(false)

}
