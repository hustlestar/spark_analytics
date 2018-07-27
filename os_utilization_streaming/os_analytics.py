from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, FloatType, StringType

spark = SparkSession \
    .builder \
    .appName("OS Monitoring In Spark") \
    .getOrCreate()

schemaExp = StructType([
    StructField("pid", StringType(), False),
    StructField("name", StringType(), True),
    StructField("cpu_percent", FloatType(), True),
    StructField("memory_percent", FloatType(), True)])
# Create DataFrame representing the stream of input lines from connection to localhost:9999
lines = spark \
    .readStream \
    .format("csv") \
    .schema(schemaExp) \
    .load("input/sys_log.csv")

# Split the lines into words
lines.printSchema()
# Generate running word count
wordCounts = lines.show()
# Start running the query that prints the running counts to the console
query = lines \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
