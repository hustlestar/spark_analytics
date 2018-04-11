package com.hustlestar.spark.view

import org.apache.spark.sql.SparkSession


object SparkUtils {

  def createSparkSession(master: String, applicationName: String): SparkSession = SparkSession
    .builder
    .appName(applicationName)
    .master("local")
    .getOrCreate
}