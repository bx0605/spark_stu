package com.sparkbyexamples.spark.study.dataframe
import org.apache.spark.sql._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
object CollectExample extends App {
  private val spark: SparkSession = SparkSession.builder.master("local").appName(this.getClass.getSimpleName).getOrCreate()
  val data = Seq(Row(Row("James ", "", "Smith"), "36636", "M", 3000),
    Row(Row("Michael ", "Rose", ""), "40288", "M", 4000),
    Row(Row("Robert ", "", "Williams"), "42114", "M", 4000),
    Row(Row("Maria ", "Anne", "Jones"), "39192", "F", 4000),
    Row(Row("Jen", "Mary", "Brown"), "", "F", -1)
  )

  val schema = new StructType().add("names", new StructType().add("fristname", StringType).add("middlename", StringType).add("lastname", StringType))
    .add("id", StringType)
    .add("gender", StringType)
    .add("salary", IntegerType)

   val frame: DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
  private val rows: Array[Row] = frame.collect()

  rows.foreach(rows=>{
    println(rows.get(0))
  })

  rows.foreach(rows=>{
    val row = rows.getStruct(0)
    val str = row.getAs[String]("fristname")
    println(str)
  })
}
