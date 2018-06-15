package com.hustlestar.spark.view

import org.apache.spark.sql.{DataFrame, SparkSession}

trait ReadWriteHelper {
  def readCsv(sparkSession: SparkSession, filePath: String): DataFrame = {
    val dataFrame: DataFrame = sparkSession.read
      .format("csv")
      .option("header", "true")
      //.option("quote", "\"")
      .option("inferSchema", "true")
      .load(filePath)
    dataFrame
  }

  def saveAsParquet(df: DataFrame, path: String): Unit = {
    df.write
      .format("parquet")
      .save(path)
  }
}

