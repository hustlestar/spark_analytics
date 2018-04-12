package com.hustlestar.spark.view

import org.apache.spark.sql.{DataFrame, SparkSession}

trait CsvCheck {
  def readCsv(sparkSession: SparkSession, filePath: String): DataFrame = {
    val dataFrame: DataFrame = sparkSession.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(filePath)
    dataFrame
  }
}

