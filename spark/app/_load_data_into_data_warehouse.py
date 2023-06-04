from pyspark.sql import SparkSession
from pyspark.sql.types import *

import sys
import datetime
import my_lib.config as config

GOLD_LAYER_PATH = config.GOLD_LAYER_PATH

bucketName = config.bucketName
postgresUrl = config.postgresUrl
postgresUser = config.postgresUser
postgresPassword = config.postgresPassword
postgresDriver = config.postgresDriver
dimDateFile = config.dimDateFile
dimSymbolFile = config.dimSymbolFile

if __name__ == "__main__":
    ACCESS_KEY = sys.argv[1]
    SECRET_KEY = sys.argv[2]
    ENDPOINT = sys.argv[3]
    newest_parts_in_gold_layer = sys.argv[4].replace('[', '').replace(']', '').replace("'", '').split(',')

    newest_part_in_fact_minute_price = newest_parts_in_gold_layer[0].strip()
    newest_part_in_fact_hour_price = newest_parts_in_gold_layer[1].strip()
    newest_part_in_fact_day_price = newest_parts_in_gold_layer[2].strip()
    
    # Create a SparkSession
    spark = SparkSession.builder \
        .master("local[*]") \
        .config("spark.hadoop.fs.s3a.endpoint", ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    postgres_properties = {
        "user": postgresUser,
        "password": postgresPassword,
        "driver": postgresDriver
    }

    # Get current time 
    now = datetime.datetime.now() + datetime.timedelta(hours=7)

    # Read data from dim_date in gold layer
    file_path_dim_date = GOLD_LAYER_PATH + "dim/" + dimDateFile
    file_path_dim_symbol = GOLD_LAYER_PATH + "dim/" + dimSymbolFile

    schema = StructType([
        StructField("Date_key", IntegerType(), True),
        StructField("TheDate", DateType(), True),
        StructField("TheDay", IntegerType(), True),
        StructField("TheDayName", StringType(), True),
        StructField("TheWeek", IntegerType(), True),
        StructField("TheDayOfWeek", IntegerType(), True),
        StructField("TheMonth", StringType(), True),
        StructField("TheMonthName", StringType(), True),
        StructField("TheQuarter", IntegerType(), True),
        StructField("TheYear", IntegerType(), True),
        StructField("TheMonthYear", StringType(), True),
        StructField("TheYearMonth", StringType(), True),
        StructField("TheQuarterYear", StringType(), True),
        StructField("TheYearQuater", StringType(), True),
        StructField("TheFirstOfMonth", DateType(), True),
        StructField("TheLastOfYear", DateType(), True)
    ])

    # dim_date
    df_dim_date = spark.read \
        .format("parquet") \
        .schema(schema) \
        .load("s3a://{}/{}".format(bucketName, file_path_dim_date))
    
    # dim_symbol
    df_dim_symbol = spark.read \
        .format("parquet") \
        .load("s3a://{}/{}".format(bucketName, file_path_dim_symbol))

    df_exist = spark.read \
        .jdbc(postgresUrl, "dim_symbol", properties=postgres_properties)

    df_temp = df_dim_symbol.union(df_exist).distinct()
    
    # fact_minute
    df_fact_minute_price = spark.read \
        .format("parquet") \
        .load("s3a://{}/{}".format(bucketName, newest_part_in_fact_minute_price))

    df_dim_date.write \
        .jdbc(postgresUrl, "dim_date", mode="overwrite", properties=postgres_properties)

    df_temp.write \
        .jdbc(postgresUrl, "dim_symbol", mode="overwrite", properties=postgres_properties)

    df_fact_minute_price.write \
        .jdbc(postgresUrl, "fact_minute_price", mode="append", properties=postgres_properties)

    # fact_hour
    # run in first minute of each hour
    if now.minute >= 0 and now.minute < 5:
        df_fact_hour_price = spark.read \
            .format("parquet") \
            .load("s3a://{}/{}".format(bucketName, newest_part_in_fact_hour_price))
        df_fact_hour_price.write \
            .jdbc(postgresUrl, "fact_hour_price", mode="append", properties=postgres_properties)

    # fact_day
    # run once time each day
    if now.hour == 11: 
        df_fact_day_price = spark.read \
                .format("parquet") \
                .load("s3a://{}/{}".format(bucketName, newest_part_in_fact_day_price))
        df_fact_day_price.write \
            .jdbc(postgresUrl, "fact_day_price", mode="append", properties=postgres_properties)

    spark.stop()
