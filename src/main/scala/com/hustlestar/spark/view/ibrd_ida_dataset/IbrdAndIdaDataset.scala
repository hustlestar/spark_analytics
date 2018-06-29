package com.hustlestar.spark.view.ibrd_ida_dataset

import java.io.File
import java.nio.file.{Files, Paths}

import com.hustlestar.spark.view.{DataFrameHelper, ReadWriteHelper, SparkUtils}
import org.apache.spark.sql.{DataFrame, SparkSession}

object IbrdAndIdaDataset {
  val loansIBRD = "D:\\Projects\\spark_data_check\\src\\main\\scala\\resources\\ibrd_ida\\IBRD_Statement_of_Loans_-_Latest_Available_Snapshot.csv"
  val outputLoansDirPath = "D:\\Projects\\spark_data_check\\src\\main\\scala\\resources\\ibrd\\"
  val creditsIDA = "D:\\Projects\\spark_data_check\\src\\main\\scala\\resources\\ibrd_ida\\IDA_Statement_of_Credits_and_Grants__-_Latest_Available_Snapshot.csv"
  val outputCreditsDirPath = "D:\\Projects\\spark_data_check\\src\\main\\scala\\resources\\ida\\"

  val spark: SparkSession = SparkUtils.getOrCreateSparkSession("International Bank for Reconstruction and Development IBRD loans, grants and credits")

  import spark.implicits._
  import org.apache.spark.sql.functions._

  def main(args: Array[String]): Unit = {
    val initialIbrdLoans: DataFrame = ReadWriteHelper.readCsvRenameColumnsAndSaveAsParquet(spark, loansIBRD, outputLoansDirPath)
    DataFrameHelper.debugDataFrame(initialIbrdLoans)

  }


}
