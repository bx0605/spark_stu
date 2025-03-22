package com.sparkbyexamples.spark.study.dataframe

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql._
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
object MapFlatMap extends App {
  private val spark: SparkSession = SparkSession
    .builder().master("local").appName(this.getClass.getSimpleName).getOrCreate()

  val data = Seq("Project Gutenberg’s",
    "Alice’s Adventures in Wonderland",
    "Project Gutenberg’s",
    "Adventures in Wonderland",
    "Project Gutenberg’s")
  import spark.implicits._

  private val dataFrame: DataFrame = data.toDF("data")
  dataFrame.show
  private val value1: Dataset[Array[String]] = dataFrame.map(x => {
    x.getString(0).split(" ")
  })
value1.show(false)
  private val value2: Dataset[String] = dataFrame.flatMap(x => {
    x.getString(0).split(" ")
  })

  value2.show(false)

  private val rows: Seq[Row] = Seq(Row("dfgbdg,g,fs,sh,", List("Java", "Scala", "C++"), "CA"), Row("dfgbdg,g,fs,sh,", List("Jasva", "Scdala", "C+s+"), "CA"), Row("dfgsdbdg,g,fs,sh,", List("Jadva", "Sgcala", "Cs++"), "CA"))

  private val schema: StructType = new StructType().add("name", StringType).add("language", ArrayType(StringType)).add("country", StringType)

  private val df: DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
  private val v3: Dataset[(String, String, String)] = df.flatMap(x => {
    val language = x.getSeq[String](1)
    language.map((x.getString(0), _, x.getString(2)))
  })

  v3.show(false)
}
