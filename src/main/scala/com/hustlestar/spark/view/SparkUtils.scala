package com.hustlestar.spark.view

import org.apache.spark.sql.{SQLContext, SparkSession}

object SparkUtils {

  def createSparkSession(applicationName: String): SparkSession = {
    val sparkSession = SparkSession
      .builder
      .appName(applicationName)
      .master("local[*]")
      .getOrCreate
    sparkSession.sparkContext.setLogLevel("ERROR")
    sparkSession
  }

  def getSqlContext(master: String, applicationName: String): SQLContext ={
    val sparkSession = createSparkSession(applicationName)
    sparkSession.sqlContext
  }
}