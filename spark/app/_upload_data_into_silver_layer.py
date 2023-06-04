from pyspark.sql import SparkSession
from pyspark.sql.types import *
import sys
import re

import my_lib.config as config
bucketName = config.bucketName

if __name__ == "__main__":
  # Read data from bronze layer
    bucket_name = bucketName
    object_name = sys.argv[1]
    file_silver_layer_path = sys.argv[2]
    ACCESS_KEY = sys.argv[3]
    SECRET_KEY = sys.argv[4]
    ENDPOINT = sys.argv[5]

    # Create a SparkSession
    spark = SparkSession.builder \
    .master("local[*]") \
    .config("spark.hadoop.fs.s3a.endpoint", ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

    dateFormat = "yyyy:MM:dd HH:mm:ss"

    schema = StructType([
        StructField("time", TimestampType(), False),
        StructField("symbol", StringType(), True),
        StructField("name", StringType(), True),
        StructField("price", FloatType(), True),
        StructField("1h%", FloatType(), True),
        StructField("24h%", FloatType(), True),
        StructField("7d%", FloatType(), True),
        StructField("market_cap", FloatType(), True),
        StructField("volume_24h", FloatType(), True),
        StructField("circulating_supply", FloatType(), True)
    ])

    df_csv = spark.read \
        .format("csv") \
        .option("header", True) \
        .option("dateFormat", dateFormat) \
        .schema(schema) \
        .load("s3a://{}/{}".format(bucket_name, object_name))

    # .save("s3a://cken-coins-data/silver/delta_table/price_coins_follow_hour")
    try:
        df_csv.write \
            .format("parquet") \
            .mode("append") \
            .option("spark.sql.parquet.writeLegacyFormat", "true") \
            .save("s3a://{}/{}".format(bucket_name, file_silver_layer_path))
    except FileNotFoundError:
        df_csv.write \
            .format("parquet") \
            .mode("overwrite") \
            .option("spark.sql.parquet.writeLegacyFormat", "true") \
            .save("s3a://{}/{}".format(bucket_name, file_silver_layer_path))

    # df_csv.write \
    #     .mode("append") \
    #     .parquet("s3a://cken-coins-data/silver/delta_table/price_coins_follow_hour")
    
    # df_parquet = spark.read \
    #     .format("parquet") \
    #     .load("s3a://cken-coins-data/silver/delta_table/price_coins_follow_minute")
    
    # df_parquet.show()
    # Stop the SparkSession
    spark.stop()
