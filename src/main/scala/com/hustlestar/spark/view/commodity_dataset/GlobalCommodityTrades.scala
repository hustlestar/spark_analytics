package com.hustlestar.spark.view.commodity_dataset

import java.nio.file.{Files, Paths}

import com.hustlestar.spark.view.{Helper, SparkUtils}
import org.apache.spark.sql.DataFrame

import scala.util.matching.Regex

object GlobalCommodityTrades {
  val helper = new Object() with Helper
  val spark = SparkUtils.createSparkSession("Explore commodity trades")
  val inputPath = "D:\\Projects\\spark_data_check\\src\\main\\scala\\resources\\global_commodity_trade_stats\\global-commodity-trade-statistics\\commodity_trade_statistics_data.csv"
  val outputRootDirPath = "D:\\Projects\\spark_data_check\\src\\main\\scala\\resources\\global_commodity_trade_stats\\"


  def analyze(df: DataFrame): Unit = {
    import spark.implicits._
    df.printSchema()
    //println(df.count())
    val distinctCountries = df.select($"country_or_area")
      .distinct()
      .filter(!$"country_or_area".rlike("[0-9]"))
      .orderBy($"country_or_area".asc)
    val cleanDF = df.select($"country_or_area")
      .distinct()
      .filter(!$"country_or_area".rlike("[0-9]"))
      .orderBy($"country_or_area".asc)
    val k = df.select($"country_or_area", $"year", $"flow")
      .distinct()
      .filter(!$"country_or_area".rlike("[0-9]"))
      .orderBy($"country_or_area".asc, $"year".asc, $"flow".asc)
      .show()
    //println(df.select($"commodity").distinct().count())
    val biggestTrade = df
      .select($"*")
      .join(distinctCountries, Seq("country_or_area"))
      .groupBy($"country_or_area", $"flow")
      .max("trade_usd")
      //.select($"country_or_area", $"flow", $"commodity", $"year", $"category", $"trade_usd")
      .show()
    //distinctCountries.explain()
    //distinctCountries.show()
  }

  def main(args: Array[String]): Unit = {
    val parquetFileLocation = outputRootDirPath + "parquet"
    // this dataset could be found at https://www.kaggle.com
    val df: DataFrame = if (!Files.exists(Paths.get(parquetFileLocation))) {
      val initialDF = helper.readCsv(spark, inputPath)
      helper.saveAsParquet(initialDF, outputRootDirPath + "parquet")
      initialDF
    } else {
      spark.read
        .parquet(parquetFileLocation)
    }
    analyze(df)
  }
}
