package com.hustlestar.spark.view.ibrd_ida_dataset

import java.io.File
import java.nio.file.{Files, Paths}

import com.hustlestar.spark.view.{DataFrameHelper, ReadWriteHelper, SparkUtils}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}

object IbrdAndIdaDataset {
  val loansIBRD = "D:\\Projects\\spark_data_check\\src\\main\\scala\\resources\\ibrd_ida\\IBRD_Statement_of_Loans_-_Latest_Available_Snapshot.csv"
  val outputLoansDirPath = "D:\\Projects\\spark_data_check\\src\\main\\scala\\resources\\ibrd\\"
  val creditsIDA = "D:\\Projects\\spark_data_check\\src\\main\\scala\\resources\\ibrd_ida\\IDA_Statement_of_Credits_and_Grants__-_Latest_Available_Snapshot.csv"
  val outputCreditsDirPath = "D:\\Projects\\spark_data_check\\src\\main\\scala\\resources\\ida\\"

  val spark: SparkSession = SparkUtils.getOrCreateSparkSession("International Bank for Reconstruction and Development IBRD loans, grants and credits")

  import spark.implicits._
  import org.apache.spark.sql.functions._

  def highestSumByCountry(df: DataFrame) = {
    df.groupBy("country")
      .agg(sum(col("original_principal_amount")).as("total_loans_amount"))
      .orderBy(col("total_loans_amount").desc)
  }

  def highestSumByCountryAndPercentFromTotal(df: DataFrame) = {
    df
      .groupBy("country")
      .agg(sum(col("original_principal_amount")).as("total_loans_amount"))
      .withColumn("total_loans_worldwide", sum("total_loans_amount").over().as("total_loans_worldwide"))
      .withColumn("percenteage_from_worldwide", col("total_loans_amount")/col("total_loans_worldwide")*100)
      .orderBy(col("total_loans_amount").desc)
  }

  def lowestInterestRatesByCountry(df: DataFrame) = {
    df.groupBy("country")
      .agg(avg(col("interest_rate")).as("avg_interest_rate"))
      .orderBy(col("avg_interest_rate").asc)
  }

  def lowestInterestRatesNumberOfLoansTotalAmountByCountry(df: DataFrame) = {
    val countryWindow = Window.partitionBy(col("country"))
    df
      .withColumn("total_loans_amount", sum(col("original_principal_amount")).over(countryWindow))
      .withColumn("avg_interest_rate", avg(col("interest_rate")).over(countryWindow))
      .withColumn("number_of_loans", count("country").over(countryWindow))
      .orderBy(col("avg_interest_rate").asc)
      .select(col("country"), col("number_of_loans"), col("avg_interest_rate"), col("total_loans_amount"))
      .distinct()
  }

  def main(args: Array[String]): Unit = {
    val initialIbrdLoans: DataFrame = ReadWriteHelper.readCsvRenameColumnsAndSaveAsParquet(spark, loansIBRD, outputLoansDirPath)
    initialIbrdLoans.cache()
    DataFrameHelper.debugDataFrame(initialIbrdLoans)
    DataFrameHelper.debugDataFrame(highestSumByCountry(initialIbrdLoans))
    DataFrameHelper.debugDataFrame(highestSumByCountryAndPercentFromTotal(initialIbrdLoans))
    DataFrameHelper.debugDataFrame(lowestInterestRatesByCountry(initialIbrdLoans))
    DataFrameHelper.debugDataFrame(lowestInterestRatesNumberOfLoansTotalAmountByCountry(initialIbrdLoans))
  }


}
