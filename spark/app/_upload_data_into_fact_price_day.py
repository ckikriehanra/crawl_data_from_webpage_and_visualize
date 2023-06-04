from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, lit, avg

import sys
import datetime

if __name__ == "__main__":
    bucket_name = sys.argv[1]
    fact_price_day_path = sys.argv[2]
    fact_price_minute_path = sys.argv[3]
    ACCESS_KEY = sys.argv[4]
    SECRET_KEY = sys.argv[5]
    ENDPOINT = sys.argv[6]

    # Create a SparkSession
    spark = SparkSession.builder \
    .master("local[*]") \
    .config("spark.hadoop.fs.s3a.endpoint", ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

    df = spark.read \
        .format("parquet") \
        .load("s3a://{}/{}".format(bucket_name, fact_price_minute_path))
    now = datetime.datetime.now() + datetime.timedelta(hours=7)
    curr_date = now.date()
    # prev_date = curr_date
    prev_date = curr_date- datetime.timedelta(days=1)

    newest_records_day = df.filter(col("time").cast("date") == prev_date) \
            .withColumn("day", lit(prev_date)) \
            .select("symbol_key", "day", "price", "volume_24h") \
            .groupBy("symbol_key", "day") \
            .agg(avg("price").alias("avg_price_by_day"), avg("volume_24h").alias("avg_volume_24h_by_day"))

    try:
        newest_records_day.write\
        .format("parquet")\
        .mode("append")\
        .save("s3a://{}/{}".format(bucket_name, fact_price_day_path))
    except FileNotFoundError:
        newest_records_day.write\
            .format("parquet")\
            .mode("overwrite")\
            .save("s3a://{}/{}".format(bucket_name, fact_price_day_path))

    spark.stop()

        
