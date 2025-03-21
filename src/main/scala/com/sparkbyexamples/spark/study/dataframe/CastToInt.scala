package com.sparkbyexamples.spark.study.dataframe

import org.apache.spark.sql._

object CastToInt{
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder
      .appName(this.getClass.getSimpleName)
      .master("local[*]").getOrCreate()
    val data = Seq(("James", 34, "true", "M", "3000.6089"),
      ("Michael", 33, "true", "F", "3300.8067"),
      ("Robert", 37, "false", "M", "5000.5034"))
    import spark.implicits._
    val df = data.toDF("firstname","age","isGraduated","gender","salary")
    df.printSchema()
    import org.apache.spark.sql.functions.col
    df.withColumn("salary",col("salary").cast("int")).printSchema()
    df.withColumn("salary",col("salary").cast("Integer")).printSchema()
    df.select(col("salary").cast("Integer").as("salarys")).printSchema

    df.selectExpr("cast(salary as int) salary","isGraduated").printSchema()
  }

}
