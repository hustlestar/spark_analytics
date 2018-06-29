package com.hustlestar.spark.view

import org.apache.spark.sql.{Column, DataFrame}

object DataFrameHelper {
  def debugDataFrame(df: DataFrame): Unit = {
    df.printSchema()
    df.explain()
    df.show()
  }

  def getAllDistinctValuesForCol(df: DataFrame, col: Column): DataFrame = {
    df.select(col)
      .distinct()
      .orderBy(col.asc)
      .toDF()
  }
}
