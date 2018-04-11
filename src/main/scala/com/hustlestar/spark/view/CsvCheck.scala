package com.hustlestar.spark.view

import org.apache.spark.sql.DataFrame

object CsvCheck {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkUtils.createSparkSession("localhost", "hello-world")
    val dataFrame: DataFrame = sparkSession.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("D:\\Projects\\spark_data_check\\src\\main\\scala\\resources\\993\\SAMPLE_biweekly_control_group_delivers20170102.csv")
    dataFrame.printSchema()
  }
}

