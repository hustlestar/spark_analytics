package com.hustlestar.spark.view.ibrd_ida_dataset

import com.hustlestar.spark.view.{DataFrameHelper, ReadWriteHelper, SparkUtils}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object IbrdAndIdaDataset {
  val loansIBRD = "D:\\Projects\\spark_data_check\\src\\main\\scala\\resources\\ibrd_ida\\IBRD_Statement_of_Loans_-_Latest_Available_Snapshot.csv"
  val outputLoansDirPath = "D:\\Projects\\spark_data_check\\src\\main\\scala\\resources\\ibrd\\"
  val creditsIDA = "D:\\Projects\\spark_data_check\\src\\main\\scala\\resources\\ibrd_ida\\IDA_Statement_of_Credits_and_Grants__-_Latest_Available_Snapshot.csv"
  val outputCreditsDirPath = "D:\\Projects\\spark_data_check\\src\\main\\scala\\resources\\ida\\"

  val spark: SparkSession = SparkUtils.getOrCreateSparkSession("International Bank for Reconstruction and Development IBRD loans, grants and credits")

  import org.apache.spark.sql.functions._

  def highestSumByCountry(df: DataFrame): Dataset[Row] = {
    df.groupBy("country")
      .agg(sum(col("original_principal_amount")).as("total_loans_amount"))
      .orderBy(col("total_loans_amount").desc)
  }

  def highestSumByCountryAndPercentFromTotal(df: DataFrame): Dataset[Row] = {
    df
      .groupBy("country")
      .agg(sum(col("original_principal_amount")).as("total_loans_amount"))
      .withColumn("total_loans_worldwide", sum("total_loans_amount").over().as("total_loans_worldwide"))
      .withColumn("percenteage_from_worldwide", col("total_loans_amount") / col("total_loans_worldwide") * 100)
      .orderBy(col("total_loans_amount").desc)
  }

  def lowestInterestRatesByCountry(df: DataFrame): Dataset[Row] = {
    df.groupBy("country")
      .agg(avg(col("interest_rate")).as("avg_interest_rate"))
      .orderBy(col("avg_interest_rate").asc)
  }

  def lowestInterestRatesNumberOfLoansTotalAmountByCountry(df: DataFrame): Dataset[Row] = {
    val countryWindow = Window.partitionBy(col("country"))
    df
      .withColumn("total_loans_amount", sum(col("original_principal_amount")).over(countryWindow))
      .withColumn("avg_interest_rate", avg(col("interest_rate")).over(countryWindow))
      .withColumn("number_of_loans", count("country").over(countryWindow))
      .orderBy(col("avg_interest_rate").asc)
      .select(col("country"), col("number_of_loans"), col("avg_interest_rate"), col("total_loans_amount"))
      .distinct()
  }

  def currentLoansByCountry(df: DataFrame): DataFrame = {
    df
      .where(col("loan_status").notEqual("Fully Repaid"))
      .where(col("loan_status").notEqual("Fully Cancelled"))
      .groupBy("country")
      .agg(sum(col("original_principal_amount")).as("total_loans_amount"))
      .withColumn("total_loans_worldwide", sum("total_loans_amount").over().as("total_loans_worldwide"))
      .withColumn("percenteage_from_worldwide", col("total_loans_amount") / col("total_loans_worldwide") * 100)
      .orderBy(col("total_loans_amount").desc)
  }

  def currentLoansDueDateByCountry(df: DataFrame): DataFrame = {
    df
      .where(col("loan_status").notEqual("Fully Repaid"))
      .where(col("loan_status").notEqual("Fully Cancelled"))
      .withColumn("total_loans_amount", sum(col("original_principal_amount")).over(Window.partitionBy("country")))
      .withColumn("avg_interest_rate", avg(col("interest_rate")).over(Window.partitionBy("country")))
      .withColumn("due_date", max(col("last_repayment_date")).over(Window.partitionBy("country")))
      .select("country", "total_loans_amount", "avg_interest_rate", "due_date")
      .distinct()
      .orderBy(col("total_loans_amount").desc)
  }

  def currentLoansByIdaAndIbdrByCountry(ibrdDf: DataFrame, idaDf: DataFrame): DataFrame = {
    ibrdDf
      .withColumnRenamed("total_loans_amount", "ibrd_loans")
      .withColumnRenamed("avg_interest_rate", "ibrd_interest_rate")
      .withColumn("ibrd_interest_rate", format_number(col("ibrd_interest_rate"), 3))
      .withColumnRenamed("due_date", "ibrd_due_date")
      .join(idaDf
        .withColumnRenamed("total_loans_amount", "ida_loans")
        .withColumnRenamed("avg_interest_rate", "ida_interest_rate")
        .withColumn("ida_interest_rate", format_number(col("ida_interest_rate"), 3))
        .withColumnRenamed("due_date", "ida_due_date")
        , "country")
      .withColumn("total_loans_amount", col("ibrd_loans") + col("ida_loans"))
      .withColumn("ibrd_part",
        datediff(to_date(col("ibrd_due_date"), "MM/dd/yyyy HH:mm:ss"), current_date()) * col("ibrd_interest_rate") * col("ibrd_loans"))
      .withColumn("ida_part",
        datediff(to_date(col("ida_due_date"), "MM/dd/yyyy HH:mm:ss"), current_date()) * col("ida_interest_rate") * col("ida_loans"))
      .withColumn("sum_part", col("ibrd_part") + col("ida_part"))
      .withColumn("avg_interest_rate",
        format_number(col("ibrd_interest_rate") * col("ibrd_part") / col("sum_part") + col("ida_interest_rate") * col("ida_part") / col("sum_part"), 3)
      )
      .select("country", "ibrd_loans", "ibrd_interest_rate", "ida_loans", "ida_interest_rate", "total_loans_amount", "avg_interest_rate")
      .orderBy(col("total_loans_amount").desc)
  }

  def main(args: Array[String]): Unit = {
    val initialIbrdLoans: DataFrame = ReadWriteHelper.readCsvRenameColumnsAndSaveAsParquet(spark, loansIBRD, outputLoansDirPath)
    initialIbrdLoans.cache()
    println("IBRD loans")
    DataFrameHelper.debugDataFrame(initialIbrdLoans)
    DataFrameHelper.debugDataFrame(highestSumByCountry(initialIbrdLoans))
    DataFrameHelper.debugDataFrame(highestSumByCountryAndPercentFromTotal(initialIbrdLoans))
    DataFrameHelper.debugDataFrame(lowestInterestRatesByCountry(initialIbrdLoans))
    DataFrameHelper.debugDataFrame(lowestInterestRatesNumberOfLoansTotalAmountByCountry(initialIbrdLoans))
    DataFrameHelper.debugDataFrame(currentLoansByCountry(initialIbrdLoans))
    val ibrdLoansDueDate = currentLoansDueDateByCountry(initialIbrdLoans)
    DataFrameHelper.debugDataFrame(ibrdLoansDueDate)
    val initialCreditsAndGrants = ReadWriteHelper.readCsvRenameColumnsAndSaveAsParquet(spark, creditsIDA, outputCreditsDirPath)
      .withColumnRenamed("service_charge_rate", "interest_rate")
      .withColumnRenamed("credit_status", "loan_status")
    initialCreditsAndGrants.cache()
    println("IDA loans")
    DataFrameHelper.debugDataFrame(initialCreditsAndGrants)
    DataFrameHelper.debugDataFrame(highestSumByCountry(initialCreditsAndGrants))
    DataFrameHelper.debugDataFrame(highestSumByCountryAndPercentFromTotal(initialCreditsAndGrants))
    DataFrameHelper.debugDataFrame(lowestInterestRatesByCountry(initialCreditsAndGrants))
    DataFrameHelper.debugDataFrame(lowestInterestRatesNumberOfLoansTotalAmountByCountry(initialCreditsAndGrants))
    DataFrameHelper.debugDataFrame(currentLoansByCountry(initialCreditsAndGrants))
    val idaLoansDueDate = currentLoansDueDateByCountry(initialCreditsAndGrants)
    DataFrameHelper.debugDataFrame(idaLoansDueDate)
    DataFrameHelper.debugDataFrame(currentLoansByIdaAndIbdrByCountry(ibrdLoansDueDate, idaLoansDueDate))

  }
}
