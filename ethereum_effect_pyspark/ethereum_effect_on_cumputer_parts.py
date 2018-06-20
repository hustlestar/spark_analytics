import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession \
    .builder \
    .appName("Ethereum Effect on computer parts Spark SQL") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()


def _read_input_csv(input_dir):
    """
    Reads all files into input dir and generates DataFrame
    :param input_dir: dir with input csv files
    :return: initial file name, generate DataFrame
    """
    input_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), input_dir)
    filtered_input_files = (os.path.join(input_dir, f) for f in os.listdir(input_dir) if f.endswith(".csv"))
    for f in filtered_input_files:
        print(f)
        df = spark.read \
            .option("header", "true") \
            .csv(f)
        yield os.path.splitext(os.path.basename(f))[0], df


def read_input_parquet(input_dir):
    """
    Reads input csv files and saves it as parquet for subsequent reads.
    :param input_dir:
    :return: dict with name of DataFrame as key and df itself as value
    """
    parquet_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), input_dir, "parquet")
    if os.path.exists(parquet_dir):
        dict_of_df = {d: spark.read.parquet(os.path.join(parquet_dir, d)) for d in os.listdir(parquet_dir)}
    else:
        dict_of_df = dict(_read_input_csv(input_dir))
        os.mkdir(parquet_dir)
        for table_name, df in dict_of_df.iteritems():
            df.write.parquet(os.path.join(parquet_dir, table_name))
    return dict_of_df


def debug_df(df):
    df.printSchema()
    df.explain()
    df.show()


def sum_crypto_rates(fact_df, dim_df):
    window_by_date = Window.partitionBy(fact_df.TimeId)
    # window_by_date_and_minable_coins = Window.partitionBy(fact_df.TimeId, fact_df.Number_Of_Mineable_Coins)
    return fact_df.join(dim_df, fact_df.CodeId == dim_df.Id) \
        .filter(dim_df.Is_Mineable == 1) \
        .drop("Is_Mineable", "CodeId", "Id") \
        .withColumn("Number_Of_Mineable_Coins", F.count("Code").over(window_by_date)) \
        .withColumn("Avg_Crypto_Rate", (fact_df.Open + fact_df.Close + fact_df.High + fact_df.Low) / 4) \
        .withColumn("Avg_Crypto_Rate",
                    F.sum("Avg_Crypto_Rate").over(Window.partitionBy("TimeId", "Number_Of_Mineable_Coins"))) \
        .drop("Open", "Close", "Low", "Code") \
        .orderBy(F.col("TimeId"))
    # .groupBy("TimeId", "Number_Of_Mineable_Coins") \
    # .sum("Avg_Crypto_Rate") \


def encriched_gpu_prices(fact_df, dim_df):
    without_trash_gpu_df = dim_df \
        .filter("Memory_Capacity >= 2")
    return fact_df.join(without_trash_gpu_df, fact_df.ProdId == without_trash_gpu_df.Id) \
        .drop("Id")


def join_gpu_prices_and_crypto_rates(gpu_prices_df, crypto_rates_df):
    return gpu_prices_df.join(crypto_rates_df, gpu_prices_df.TimeId == crypto_rates_df.TimeId) \
        .drop(crypto_rates_df.TimeId) \
        .withColumn("Coin_Map",
                    F.collect_list(F.create_map(F.col("Currency_Name"), F.col("High"))).over(
                        Window.partitionBy("ProdId", "TimeId", "MerchantId"))
                    ) \
        .withColumn("Coin_Map", F.col("Coin_Map").cast("string")) \
        .drop("High", "Currency_Name") \
        .distinct() \
        .orderBy("ProdId", "TimeId")


def filter_by_processor_man(df, man):
    product_time_window = Window.partitionBy("ProdId", "TimeId")
    return df \
        .withColumn("Price_USD", F.avg(F.col("Price_USD")).over(product_time_window)) \
        .withColumn("Price_Original", F.avg(F.col("Price_Original")).over(product_time_window)) \
        .drop("RegionId", "MerchantId") \
        .distinct() \
        .filter(df.Processor_Manufacturer == man)


def aggregates_by_window(df, time_dim_df, window=None):
    time_dim_df = time_dim_df.select("Id", "Year")
    year_product_window = Window.partitionBy(F.col("ProdId"), F.col("Year"))
    return df.join(time_dim_df, df.TimeId == time_dim_df.Id) \
        .withColumn("Avg_Price_USD", F.avg(F.col("Price_USD")).over(year_product_window)) \
        .withColumn("Avg_Price_Original", F.avg("Price_Original").over(year_product_window)) \
        .withColumnRenamed("Avg_Crypto_Rate", "Daily_Avg_Crypto_Rate") \
        .withColumn("Year_Avg_Crypto_Rate", F.avg("Daily_Avg_Crypto_Rate").over(Window.partitionBy("Year"))) \
        .select("TimeId",
                F.col("Processor_Manufacturer").alias("CPU_Producer"),
                "Processor",
                F.round(F.col("Memory_Capacity"), 0).alias("GB"),
                F.col("GPU_Manufacturer").alias("GPU_Producer"),
                "Number_Of_Mineable_Coins",
                F.round("Daily_Avg_Crypto_Rate", 2).alias("Daily_Avg_Crypto_Rate"),
                F.round("Year_Avg_Crypto_Rate", 2).alias("Year_Avg_Crypto_Rate"),
                F.round("Price_USD", 2).alias("Price_USD"),
                F.round("Price_Original", 2).alias("Price_Original"),
                F.round("Avg_Price_USD", 2).alias("Avg_Price_USD"))


if __name__ == '__main__':
    input_dir = "ethereum-effect-pc-parts"
    df_dict = read_input_parquet(input_dir)
    for name, df in df_dict.iteritems():
        print("Some basic info about {}".format(name))
        # debug_df(df)
    crypto_rates_df = sum_crypto_rates(df_dict["FACT_CRYPTO_RATE"], df_dict["DIM_CRYPTO_DATA"])
    gpu_prices_df = encriched_gpu_prices(df_dict["FACT_GPU_PRICE"], df_dict["DIM_GPU_PROD"])
    print gpu_prices_df.show()
    crypto_rates_vs_gpu_prices_df = join_gpu_prices_and_crypto_rates(gpu_prices_df, crypto_rates_df)
    nvidia_df = filter_by_processor_man(crypto_rates_vs_gpu_prices_df, "NVidia")
    nvidia_df.cache()
    nvidia_df.show()
    print nvidia_df.count()
    rates_vs_avg_prices_df = aggregates_by_window(nvidia_df, df_dict["DIM_TIME"])
    rates_vs_avg_prices_df.show(200)
