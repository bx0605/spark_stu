package com.sparkbyexamples.spark.study.dataframe
import org.apache.spark.sql._
import org.apache.spark.sql.types._
object DataFrameComplex extends App {
  private val spark: SparkSession = SparkSession.builder.appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()

  val structureData = Seq(
    Row(Row("James", "", "Smith"), "36636", "NewYork", 3100, List("Java", "Scala"), Map("hair" -> "black", "eye" -> "brown")),
    Row(Row("Michael", "Rose", ""), "40288", "California", 4300, List("Python", "PHP"), Map("hair" -> "black", "eye" -> "brown")),
    Row(Row("Robert", "", "Williams"), "42114", "Florida", 1400, List("C++", "C#"), Map("hair" -> "black", "eye" -> "brown")),
    Row(Row("Maria", "Anne", "Jones"), "39192", "Florida", 5500, List("Python", "Scala"), Map("hair" -> "black", "eye" -> "brown")),
    Row(Row("Jen", "Mary", "Brown"), "34561", "NewYork", 3000, List("R", "Scala"), Map("hair" -> "black", "eye" -> "brown"))
  )
  private val schema: StructType = new StructType().add("name", new StructType().add("firstname", StringType)
      .add("middlename", StringType)
      .add("lastname", StringType))
    .add("id", StringType)
    .add("location", StringType)
    .add("salary", IntegerType)
    .add("languagesKnown", ArrayType(StringType))
    .add("properties", MapType(StringType, StringType))

  private val frame: DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(structureData), schema)
  frame.printSchema()
  frame.show(false)

}
