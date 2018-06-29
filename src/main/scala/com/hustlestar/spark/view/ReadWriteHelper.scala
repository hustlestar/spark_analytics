package com.hustlestar.spark.view

import java.io.File
import java.nio.file.{Files, Paths}

import org.apache.spark.sql.{DataFrame, SparkSession}

object ReadWriteHelper {
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

  def readCsvAndSaveAsParquet(spark: SparkSession, inputPath: String, outputDir: String): DataFrame = {
    val parquetFileLocation = new File(outputDir, "parquet").toString()
    val df: DataFrame = if (!Files.exists(Paths.get(parquetFileLocation))) {
      val initialDF = this.readCsv(spark, inputPath)
      this.saveAsParquet(initialDF, parquetFileLocation)
      initialDF
    } else {
      spark.read.parquet(parquetFileLocation)
    }
    df
  }

  def readCsvRenameColumnsAndSaveAsParquet(spark: SparkSession, inputPath: String, outputDir: String): DataFrame = {
    val outputPaquetDir = new File(outputDir, "parquet").toString
    val initialIbrdLoans = if (!Files.exists(Paths.get(outputPaquetDir))) {
      var df = ReadWriteHelper.readCsv(spark, inputPath)
      val validColNames = df.columns.map(s => s.replaceAll(" ", "_").replaceAll("[()]", "").toLowerCase)
      for ((existingColName, newColName) <- df.columns zip validColNames) {
        df = df.withColumnRenamed(existingColName, newColName)
      }
      ReadWriteHelper.saveAsParquet(df, outputPaquetDir)
      df
    } else {
      ReadWriteHelper.readCsvAndSaveAsParquet(spark, inputPath, outputDir)
    }
    initialIbrdLoans
  }
}

