package com.hustlestar.spark.view.commodity_dataset

import java.nio.file.{Files, Paths}

import com.hustlestar.spark.view.{DataFrameHelper, ReadWriteHelper, SparkUtils}
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window, WindowSpec}
import org.apache.spark.sql.{DataFrame, SparkSession}

object GlobalCommodityTrades {
  val inputPath = "D:\\Projects\\spark_data_check\\src\\main\\scala\\resources\\global_commodity_trade_stats\\global-commodity-trade-statistics\\commodity_trade_statistics_data.csv"
  val outputRootDirPath = "D:\\Projects\\spark_data_check\\src\\main\\scala\\resources\\global_commodity_trade_stats\\"

  val IO_HELPER = new Object() with ReadWriteHelper
  val DF_HELPER = new Object() with DataFrameHelper
  val spark: SparkSession = SparkUtils.createSparkSession("Explore commodity trades")

  import spark.implicits._
  import org.apache.spark.sql.functions._

  def cleanInitialDF(df: DataFrame): DataFrame = {
    df
      .filter(!$"country_or_area".rlike("[0-9]"))
      .filter($"flow".isNotNull)
      .select($"*")
  }

  val convertLongToUsd: UserDefinedFunction = udf(
    (arg: Long) => arg match {
      case _ if arg / 1000000000000L > 1 => arg / 10000000000L + " tri"
      case _ if arg / 1000000000 > 1 => arg / 1000000000 + " bil"
      case _ if arg / 1000000 > 1 => arg / 1000000 + " mil"
      case _ => arg.toString
    })

  def biggestTradeInUsdForCountryFlowEver2(df: DataFrame): DataFrame = {
    val windowSpec: WindowSpec = Window.partitionBy($"country_or_area", $"flow").orderBy($"country_or_area", $"flow")
    df
      .select($"*")
      .filter($"comm_code" =!= "TOTAL")
      .withColumn("biggest_trade", max($"trade_usd").over(windowSpec))
      .filter($"biggest_trade" === $"trade_usd")
      .orderBy($"biggest_trade".desc)
      .withColumn("biggest_trade", convertLongToUsd($"biggest_trade"))
  }

  def mostTradedCommoditiesListDesc(df: DataFrame): DataFrame = {
    df
      .filter($"commodity" =!= "ALL COMMODITIES")
      .groupBy($"commodity", $"flow")
      .agg(sum("trade_usd").as("overall_sum"))
      .orderBy($"overall_sum".desc)
      .withColumn("overall_sum", convertLongToUsd($"overall_sum"))
  }

  def main(args: Array[String]): Unit = {
    val parquetFileLocation = outputRootDirPath + "parquet"
    // this dataset could be found at https://www.kaggle.com
    val df: DataFrame = if (!Files.exists(Paths.get(parquetFileLocation))) {
      val initialDF = IO_HELPER.readCsv(spark, inputPath)
      IO_HELPER.saveAsParquet(initialDF, outputRootDirPath + "parquet")
      initialDF
    } else {
      spark.read.parquet(parquetFileLocation)
    }
    val cleanedDF = cleanInitialDF(df)
    cleanedDF.cache()
    DF_HELPER.debugDataFrame(cleanedDF)
    val biggestTrade2 = biggestTradeInUsdForCountryFlowEver2(cleanedDF)
    DF_HELPER.debugDataFrame(biggestTrade2)
    val mostTradedCommodityRating = mostTradedCommoditiesListDesc(cleanedDF)
    DF_HELPER.debugDataFrame(mostTradedCommodityRating)
  }
}
