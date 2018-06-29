package com.hustlestar.spark.view._993

import com.hustlestar.spark.view.{ReadWriteHelper, SparkUtils}
import org.apache.spark.sql.DataFrame


object BiweeklyCGCGDelivers extends BiweeklyCGLogic {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkUtils.getOrCreateSparkSession("biweekly_control_group_delivers")
    val dataFrame: DataFrame = ReadWriteHelper.readCsv(sparkSession, "D:\\Projects\\spark_data_check\\src\\main\\scala\\resources\\993\\SAMPLE_biweekly_control_group_delivers20170102.csv")
    dataFrame.printSchema()
    showDuplicatesForColumn(dataFrame, ATG_ID)
    showDuplicatesForColumn(dataFrame, EMAIL)
    showDuplicatesForColumn(dataFrame, CUSTOMER_KEY)
  }
}
