from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, hour, lit, avg

import sys
import datetime

if __name__ == "__main__":
    bucket_name = sys.argv[1]
    fact_price_hour_path = sys.argv[2]
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
    
    curr_hour = now.hour
    curr_date = now.date()
    # prev_hour = curr_hour
    prev_hour = curr_hour-1
    if prev_hour < 0:
        prev_hour += 24
        curr_date = curr_date - datetime.timedelta(days=1)

    newest_records_hour_day = df.filter((hour(col("time")) == prev_hour) & (col("time").cast("date") == curr_date)) \
        .withColumn("hour", lit(prev_hour)) \
        .withColumn("day", lit(curr_date)) \
        .select("symbol_key", "hour", "day", "price", "volume_24h") \
        .groupBy("symbol_key", "hour", "day") \
        .agg(avg("price").alias("avg_price_by_hour"), avg("volume_24h").alias("avg_volume_24h_by_hour"))
    
    # newest_records_hour_day.show()
    
    try:
        newest_records_hour_day.write\
            .format("parquet")\
            .mode("append")\
            .save("s3a://{}/{}".format(bucket_name, fact_price_hour_path))
    except FileNotFoundError:
        newest_records_hour_day.write\
            .format("parquet")\
            .mode("overwrite")\
            .save("s3a://{}/{}".format(bucket_name, fact_price_hour_path))
    spark.stop()
    
    
