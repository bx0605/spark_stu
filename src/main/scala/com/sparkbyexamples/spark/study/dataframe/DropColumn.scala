package com.sparkbyexamples.spark.study.dataframe
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
object DropColumn extends App {
  private val spark: SparkSession = SparkSession.builder.master("local[1]").appName("f").getOrCreate()
  val data = Seq(
    Row("James", "", "Smith", "36636", "NewYork", 3100),
    Row("Michael", "Rose", "", "40288", "California", 4300),
    Row("Robert", "", "Williams", "42114", "Florida", 1400),
    Row("Maria", "Anne", "Jones", "39192", "Florida", 5500),
    Row("Jen", "Mary", "Brown", "34561", "NewYork", 3000)
  )

  val schema = new StructType()
    .add("firstname",StringType)
    .add("middlename",StringType)
    .add("lastname",StringType)
    .add("id",StringType)
    .add("location",StringType)
    .add("salary",IntegerType)

  private val frame: DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
  frame.show(false)
  import spark.implicits._
  frame.drop(col("id")).show
  frame.drop("id").show
  private val seq: Seq[String] = Seq("firstname", "location")
  frame.drop(seq:_*).printSchema()
}
