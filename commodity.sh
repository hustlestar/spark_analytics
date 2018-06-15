#!/bin/bash

PWD=$(pwd)

./bin/spark-submit \
  --class com.hustlestar.spark.view.commodity_dataset.GlobalCommodityTrades \
  --master local[8] \
  $PWD/target/examples.jar \