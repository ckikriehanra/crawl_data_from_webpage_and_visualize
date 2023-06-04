from pyspark.sql import SparkSession
from pyspark.sql.types import *
import sys
from pyspark.sql.functions import col, abs, hash, to_date
    


if __name__ == "__main__":
    # Read data from bronze layer
    file_name_in_parquet = sys.argv[1]  
    bucket_name = sys.argv[2]
    dim_symbol_path = sys.argv[3]
    dim_date_path = sys.argv[4]
    fact_minute_price = sys.argv[5]
    ACCESS_KEY = sys.argv[6]
    SECRET_KEY = sys.argv[7]
    ENDPOINT = sys.argv[8]

    # Create a SparkSession
    spark = SparkSession.builder \
    .master("local[*]") \
    .config("spark.hadoop.fs.s3a.endpoint", ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

    schema = StructType([
        StructField("time", TimestampType(), True),
        StructField("symbol", StringType(), False),
        StructField("name", StringType(), False),
        StructField("price", FloatType(), True),
        StructField("1h%", FloatType(), True),
        StructField("24h%", FloatType(), True),
        StructField("7d%", FloatType(), True),
        StructField("market_cap", FloatType(), True),
        StructField("volume_24h", FloatType(), True),
        StructField("circulating_supply", FloatType(), True)
    ])

    schema_dim_date = StructType([
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
 
    schema_dim_symbol = StructType([
        StructField("symbol_key", IntegerType(), False),
        StructField("symbol", StringType(), True),
        StructField("name", StringType(), True)
    ]) 


        
    df_parquet = spark.read \
        .format("parquet") \
        .schema(schema) \
        .load("s3a://{}/{}".format(bucket_name, file_name_in_parquet))
    
    df_dim_date = spark.read \
        .format("parquet") \
        .schema(schema_dim_date) \
        .load("s3a://{}/{}".format(bucket_name, dim_date_path))

    # Create key
    df_temp = (df_parquet.withColumn("symbol_key", (abs(hash(col("name"))) % 100000000))).select("symbol_key", "symbol", "name")
    # print("haha:{}".format(df_temp.count()))

    
    # Load exist dim_symbol
    df_exist = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema_dim_symbol)
    try:
        df_exist = spark.read \
            .format("parquet") \
            .schema(schema_dim_symbol) \
            .load("s3a://{}/{}".format(bucket_name, dim_symbol_path))
    except:
        pass
    
    # Update dim_symbol
    df_result = df_exist.union(df_temp).distinct()
    
    # Load data into fact table
    df_fact = (df_result.join(df_parquet, (df_parquet.symbol==df_result.symbol) & (df_parquet.name==df_result.name), how="inner")) \
        .join(df_dim_date, to_date(col("time"))==df_dim_date.TheDate, how="inner") \
        .select ("symbol_key", "Date_key", "time", "price", "1h%", "24h%", "7d%", "market_cap", "volume_24h", "circulating_supply")

    # Update data in gold layer
    # Update fact_hour_exchange_rate
    try :
        df_fact.write \
            .format("parquet") \
            .mode("append") \
            .save("s3a://{}/{}".format(bucket_name, fact_minute_price))
    except FileNotFoundError:
        df_fact.write \
        .format("parquet") \
        .mode("overwrite") \
        .save("s3a://{}/{}".format(bucket_name, fact_minute_price))

    # Update dim_symbol
    df_result.write \
        .format("parquet") \
        .mode("overwrite") \
        .save("s3a://{}/{}".format(bucket_name, dim_symbol_path))
    
    
    # Stop the SparkSession
    spark.stop()
