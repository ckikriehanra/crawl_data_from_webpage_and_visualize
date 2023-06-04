from pyspark.sql import SparkSession
from pyspark.sql.types import *

import sys
import my_lib.config as config
bucketName = config.bucketName


if __name__ == "__main__":
    ACCESS_KEY = sys.argv[1]
    SECRET_KEY = sys.argv[2]
    ENDPOINT = sys.argv[3]

    # Create a SparkSession
    spark = SparkSession.builder \
        .master("local[*]") \
        .config("spark.hadoop.fs.s3a.endpoint", ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()


    # Read data from bronze layer
    bucket_name = bucketName

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

    df_csv = spark.read \
        .option("header", True) \
        .schema(schema) \
        .csv("/usr/local/spark/resources/data/dim_date.csv")

    df_csv.write \
        .format("parquet") \
        .mode("overwrite") \
        .save("s3a://{}/gold/dim/dim_date".format(bucket_name))
    # df_parquet = spark.read \
    #     .format("parquet") \
    #     .load("s3a://cken-coins-data/gold/dim/dim_date")

    # df_parquet.show()
    # Stop the SparkSession
    spark.stop()
