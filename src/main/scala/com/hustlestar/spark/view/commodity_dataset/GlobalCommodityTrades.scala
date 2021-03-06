package com.hustlestar.spark.view.commodity_dataset

import com.hustlestar.spark.view.{DataFrameHelper, ReadWriteHelper, SparkUtils}
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window, WindowSpec}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object GlobalCommodityTrades {
  val inputPath = "D:\\Projects\\spark_data_check\\src\\main\\scala\\resources\\global_commodity_trade_stats\\global-commodity-trade-statistics\\commodity_trade_statistics_data.csv"
  val outputRootDirPath = "D:\\Projects\\spark_data_check\\src\\main\\scala\\resources\\global_commodity_trade_stats\\"

  val spark: SparkSession = SparkUtils.getOrCreateSparkSession("Explore commodity trades")

  import org.apache.spark.sql.functions._
  import spark.implicits._

  def cleanInitialDF(df: DataFrame): DataFrame = {
    df
      .filter(!$"country_or_area".rlike("[0-9]"))
      .filter($"flow".isNotNull)
  }

  val convertLongToUsd: UserDefinedFunction = udf(
    (arg: Long) => arg match {
      case _ if Math.abs(arg / 1000000000000L) > 0 => arg / 1000000000000L + " tri"
      case _ if Math.abs(arg / 1000000000) > 0 => arg / 1000000000 + " bil"
      case _ if Math.abs(arg / 1000000) > 0 => arg / 1000000 + " mil"
      case _ => arg.toString
    })

  def biggestTradeInUsdForCountryFlowEver2(df: DataFrame): DataFrame = {
    val windowSpec: WindowSpec = Window.partitionBy($"country_or_area", $"flow").orderBy($"country_or_area", $"flow")
    df
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

  def mostTradedCategoriesListDesc(df: DataFrame): DataFrame = {
    df
      .filter($"category" =!= "all_commodities")
      .groupBy($"category", $"flow")
      .agg(sum("trade_usd").as("overall_sum"))
      .orderBy($"overall_sum".desc)
      .withColumn("overall_sum", convertLongToUsd($"overall_sum"))
  }

  def mostTradedCommodityOverCategoryListDesc(df: DataFrame): DataFrame = {
    val categoryFlowSpec: WindowSpec = Window.partitionBy($"category", $"flow")
    val categoryFlowOverallSumSpec: WindowSpec = Window.partitionBy($"category", $"flow", $"overall_sum")
    df
      .filter($"category" =!= "all_commodities")
      .withColumn("overall_sum", sum($"trade_usd").over(categoryFlowSpec))
      .groupBy($"commodity", $"category", $"flow", $"overall_sum")
      .agg(sum($"trade_usd").as("sum_for_good"))
      .withColumn("top", max("sum_for_good").over(categoryFlowOverallSumSpec))
      .filter($"top" === $"sum_for_good")
      .select($"commodity", $"flow", $"category", $"overall_sum", $"sum_for_good")
      .orderBy($"overall_sum".desc)
      .withColumn("overall_sum", convertLongToUsd($"overall_sum"))
      .withColumn("sum_for_good", convertLongToUsd($"sum_for_good"))
  }

  def countriesWhichImportExportRatingDesc(df: DataFrame): DataFrame = {
    val countryYearFlowSpec: WindowSpec = Window.partitionBy($"country_or_area", $"year", $"flow")
    //val categoryFlowOverallSumSpec: WindowSpec = Window.partitionBy($"category", $"flow", $"overall_sum")
    df
      .withColumn("overall_sum", sum($"trade_usd").over(countryYearFlowSpec))
      .select($"country_or_area", $"flow", $"year", $"overall_sum")
      .distinct()
      .orderBy($"overall_sum".desc)
      .withColumn("overall_sum_str", convertLongToUsd($"overall_sum"))
  }

  def countriesTotalImportSince2010RatingDesc(df: DataFrame): DataFrame = {
    val countryFlowSpec: WindowSpec = Window.partitionBy($"country_or_area", $"flow")
    df
      .withColumn("year", $"year".cast(IntegerType))
      .filter($"year" >= 2010)
      .filter($"flow" === "Import")
      .withColumn("overall_sum", sum($"trade_usd").over(countryFlowSpec))
      .select($"country_or_area", $"flow", $"overall_sum")
      .distinct()
      .orderBy($"overall_sum".desc)
      .withColumn("overall_sum_str", convertLongToUsd($"overall_sum"))
  }

  def countriesTotalExportSince2010RatingDesc(df: DataFrame): DataFrame = {
    val countryFlowSpec: WindowSpec = Window.partitionBy($"country_or_area", $"flow")
    df
      .withColumn("year", $"year".cast(IntegerType))
      .filter($"year" >= 2010)
      .filter($"flow" === "Export")
      .withColumn("overall_sum", sum($"trade_usd").over(countryFlowSpec))
      .select($"country_or_area", $"flow", $"overall_sum")
      .distinct()
      .orderBy($"overall_sum".desc)
      .withColumn("overall_sum_str", convertLongToUsd($"overall_sum"))
  }


  def importExportSaldoSince2010(exportersRatingSince2010: DataFrame, importersRatingSince2010: DataFrame): DataFrame = {
    val importReport = importersRatingSince2010.select($"country_or_area", $"overall_sum".as("overall_import"))
    exportersRatingSince2010
      .select($"country_or_area", $"overall_sum".as("overall_export"))
      .join(importReport, Seq("country_or_area"))
      .withColumn("saldo", $"overall_export" - $"overall_import")
      .withColumn("saldo_str", convertLongToUsd($"saldo"))
      .orderBy($"country_or_area".asc)
  }

  def saldoDynamicsForCountryYearly(df: DataFrame, country: String): Dataset[Row] = {
    val countryYearSpec: WindowSpec = Window.partitionBy($"country_or_area", $"year").orderBy($"flow", $"year")
    df
      .filter($"country_or_area" === country)
      .filter($"flow" === "Import" || $"flow" === "Export")
      .groupBy($"country_or_area", $"flow", $"year")
      .agg(sum($"trade_usd").as("export_total"))
      .withColumn("import_total", lag("export_total", -1).over(countryYearSpec))
      .withColumn("saldo", $"export_total" - $"import_total")
      .filter($"saldo".isNotNull)
      .drop("flow")
      .withColumn("saldo_str", convertLongToUsd($"saldo"))
      .orderBy($"year".asc)
  }

  def main(args: Array[String]): Unit = {
    val df: DataFrame = ReadWriteHelper.readCsvAndSaveAsParquet(spark, inputPath, outputRootDirPath)
    val cleanedDF = cleanInitialDF(df)
    cleanedDF.cache()
    DataFrameHelper.debugDataFrame(cleanedDF)
    val biggestTrade2 = biggestTradeInUsdForCountryFlowEver2(cleanedDF)
    DataFrameHelper.debugDataFrame(biggestTrade2)
    val mostTradedCommodityRating = mostTradedCommoditiesListDesc(cleanedDF)
    DataFrameHelper.debugDataFrame(mostTradedCommodityRating)
    val mostTradedCategoryRating = mostTradedCategoriesListDesc(cleanedDF)
    DataFrameHelper.debugDataFrame(mostTradedCategoryRating)
    val mostTradedGoodInCategoryRating = mostTradedCommodityOverCategoryListDesc(cleanedDF)
    mostTradedCommodityOverCategoryListDesc(cleanedDF).show(50)
    DataFrameHelper.debugDataFrame(mostTradedGoodInCategoryRating)
    val biggestImportersExporters = countriesWhichImportExportRatingDesc(cleanedDF)
    biggestImportersExporters.show(50)
    val importersRatingSince2010 = countriesTotalImportSince2010RatingDesc(cleanedDF)
    DataFrameHelper.debugDataFrame(importersRatingSince2010)
    val exportersRatingSince2010 = countriesTotalExportSince2010RatingDesc(cleanedDF)
    DataFrameHelper.debugDataFrame(exportersRatingSince2010)

    val saldoSince2010 = importExportSaldoSince2010(exportersRatingSince2010, importersRatingSince2010)
    DataFrameHelper.debugDataFrame(saldoSince2010)
    saldoSince2010.cache()
    saldoSince2010.show(200)
    saldoSince2010.orderBy($"saldo".desc).show(200)

    val belarusSaldoDynamics = saldoDynamicsForCountryYearly(cleanedDF, "Belarus")
    val usaSaldoDynamics = saldoDynamicsForCountryYearly(cleanedDF, "China")
    usaSaldoDynamics.show(30)
    val chinaSaldoDynamics = saldoDynamicsForCountryYearly(cleanedDF, "USA")
    chinaSaldoDynamics.show(30)
    val germanySaldoDynamics = saldoDynamicsForCountryYearly(cleanedDF, "Germany")
    germanySaldoDynamics.show(30)
    val russiaSaldoDynamics = saldoDynamicsForCountryYearly(cleanedDF, "Russian Federation")
    russiaSaldoDynamics.show(30)
    DataFrameHelper.debugDataFrame(belarusSaldoDynamics)
    belarusSaldoDynamics.show(50)
  }
}
