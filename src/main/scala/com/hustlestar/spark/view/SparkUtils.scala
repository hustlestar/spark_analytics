package com.hustlestar.spark.view

import org.apache.spark.sql.SparkSession


object SparkUtils {

  def createSparkSession(master: String, applicationName: String): SparkSession = {
    val sparkSession = SparkSession
      .builder
      .appName(applicationName)
      .master("local")
      .getOrCreate
    sparkSession.sparkContext.setLogLevel("ERROR")
    sparkSession
  }
}