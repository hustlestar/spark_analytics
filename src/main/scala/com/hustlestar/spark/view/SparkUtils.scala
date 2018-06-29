package com.hustlestar.spark.view

import org.apache.spark.sql.{SQLContext, SparkSession}

object SparkUtils {
  var spark: SparkSession = _

  def getOrCreateSparkSession(applicationName: String): SparkSession = {
    if (spark == null ) {
      val spark = SparkSession
        .builder
        .appName(applicationName)
        .master("local[*]")
        .getOrCreate
      spark.sparkContext.setLogLevel("ERROR")
      spark
    } else {
      spark
    }
  }

  def getSqlContext(master: String, applicationName: String): SQLContext = {
    val sparkSession = getOrCreateSparkSession(applicationName)
    sparkSession.sqlContext
  }
}