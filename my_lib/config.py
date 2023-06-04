import os

# Start page
start_page = 1

# End page
end_page  = 5# maximum of end page is 101



# Folder in minio
BRONZE_LAYER_PATH = "bronze/coin_market/minute_crawled_data/"
SILVER_LAYER_PATH = "silver/delta_table/"
GOLD_LAYER_PATH = "gold/"

# Info to login Minio
accessKey="kirihara"
secretKey="minioadmin"
endpoint="http://minio1:9000"

# Bucket name of minio
bucketName = "cken-coins-data"

# File name of flat file in silver layer on minio
flatFileSilver = "price_coins_follow_minute"

# File path of fact in gold layer
factMinutePrice = "fact/fact_minute_price"
factHourPrice = "fact/fact_hour_price"
factDayPrice = "fact/fact_day_price"

# File name of fact and dim table on minio
factPriceMinute = "fact_minute_price"
factPriceHour = "fact_hour_price"
factPriceDay = "fact_day_price"
dimSymbolFile = "dim_symbol"
dimDateFile = "dim_date"


# Folder on local
RAW_PATH = "./my_data/raw_data/"
CLEAN_PATH = "./my_data/clean_data/"
STAGE_PATH = "./my_data/stage_layer/"

# File on local
tempFile = "temp_file.csv"
tempFileParquet = "temp_file.parquet"

# Postgres for datawarehouse
postgresHost = "172.28.0.6"
postgresPort = "5432"
postgresDatabaseName = "postgres"
postgresUrl  = f"jdbc:postgresql://{postgresHost}:{postgresPort}/{postgresDatabaseName}"

# Postgres metadata
postgresUser = "airflow"
postgresPassword = "airflow"
postgresDriver = "org.postgresql.Driver"

