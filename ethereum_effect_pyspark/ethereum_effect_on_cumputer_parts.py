from pyspark.sql import SparkSession
import os

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
    parquet_dir = os.path.join(input_dir, "parquet")
    if os.path.exists(parquet_dir):
        dict_of_df = {d: spark.read.parquet(os.path.join(parquet_dir, d)) for d in os.listdir(parquet_dir)}
    else:
        dict_of_df = dict(_read_input_csv(input_dir))
        os.mkdir(parquet_dir)
        for table_name, df in dict_of_df.iteritems():
            df.write.parquet(os.path.join(parquet_dir, table_name))
    return dict_of_df


if __name__ == '__main__':
    input_dir = "ethereum-effect-pc-parts"
    print(read_input_parquet(input_dir))

