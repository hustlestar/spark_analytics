package com.hustlestar.spark.view._993

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

trait BiweeklyCGLogic{
  val N_OF_OCCURRENCES = "number_of_occurrences"
  val ATG_ID = "atg_id"
  val EMAIL = "email"
  val CUSTOMER_KEY = "customer_key"

  def showDuplicatesForColumn(dataFrame: DataFrame, columnName: String): Unit = {
    dataFrame.groupBy(columnName)
      .count()
      .withColumnRenamed("count", N_OF_OCCURRENCES)
      .where(col(N_OF_OCCURRENCES) > 1)
      .select(columnName, N_OF_OCCURRENCES)
      .show(50)
  }
}
