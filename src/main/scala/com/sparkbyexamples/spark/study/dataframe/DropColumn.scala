package com.sparkbyexamples.spark.study.dataframe
import org.apache.spark.sql._
import org.apache.spark.sql.types._
object DropColumn extends App {
  SparkSession.builder.master("local[1]").appName("f").getOrCreate()
  val data = Seq(
    Row("James", "", "Smith", "36636", "NewYork", 3100),
    Row("Michael", "Rose", "", "40288", "California", 4300),
    Row("Robert", "", "Williams", "42114", "Florida", 1400),
    Row("Maria", "Anne", "Jones", "39192", "Florida", 5500),
    Row("Jen", "Mary", "Brown", "34561", "NewYork", 3000)
  )

  val schema=new StructType().add()
}
