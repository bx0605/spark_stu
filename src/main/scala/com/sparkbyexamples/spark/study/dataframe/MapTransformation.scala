package com.sparkbyexamples.spark.study.dataframe

import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object MapTransformation extends App {
  private val spark: SparkSession = SparkSession.builder().master("local").appName(this.getClass.getSimpleName).getOrCreate()

  val structureData = Seq(
    Row("James","","Smith","36636","NewYork",3100),
    Row("Michael","Rose","","40288","California",4300),
    Row("Robert","","Williams","42114","Florida",1400),
    Row("Maria","Anne","Jones","39192","Florida",5500),
    Row("Jen","Mary","Brown","34561","NewYork",3000)
  )

  val structureSchema = new StructType()
    .add("firstname",StringType)
    .add("middlename",StringType)
    .add("lastname",StringType)
    .add("id",StringType)
    .add("location",StringType)
    .add("salary",IntegerType)
  import spark.implicits._
  private val frame: DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(structureData), structureSchema)
  private val value: Dataset[(String, String, Int)] = frame.mapPartitions(iterator => {
    val util = new Util()
    val tuples = iterator.map(row => {
      val fullName = util.combine(row.getString(0), row.getString(1), row.getString(2))
      (fullName, row.getString(3), row.getInt(5))
    })
    tuples
  })
  value.show(false)
}
