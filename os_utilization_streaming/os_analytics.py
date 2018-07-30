from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType, StringType
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession \
    .builder \
    .appName("OS Monitoring In Spark") \
    .getOrCreate()

schemaExp = StructType([
    StructField("pid", StringType(), False),
    StructField("name", StringType(), True),
    StructField("cpu_percent", DoubleType(), True),
    StructField("memory_percent", DoubleType(), True)])
# Create DataFrame representing the stream of input lines from connection to localhost:9999
lines = spark \
    .readStream \
    .format("csv") \
    .schema(schemaExp) \
    .load("input/sys_log*.csv")

# Split the lines into words
lines.printSchema()
# Generate running word count
window_by_name = Window.partitionBy(F.col("name"))
process_counts = lines \
    .groupBy("name")\
    .sum("memory_percent")\
    .withColumnRenamed("sum(memory_percent)", "total_memory_percent")\
    .orderBy("total_memory_percent", ascending=False)

    #.withColumn("total_memory_percent", F.sum("memory_percent").over(window_by_name)) \
    #.withColumn("total_cpu_percent", F.sum("cpu_percent").over(window_by_name))\

# Start running the query that prints the running counts to the console
query = process_counts \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("numRows", 50) \
    .start()

query.awaitTermination()
