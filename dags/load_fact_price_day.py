from airflow import DAG 
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
from my_lib import config

spark_master = "spark://spark:7077"
spark_app_name = "upload_data_into_fact_price_day"

bucket_name = config.bucketName
fact_price_day_path = config.GOLD_LAYER_PATH + "fact/" + config.factPriceDay
fact_price_minute_path = config.GOLD_LAYER_PATH + "fact/" + config.factPriceMinute

ACCESS_KEY = config.accessKey
SECRET_KEY = config.secretKey
ENDPOINT = config.endpoint

with DAG("load_data_into_fact_price_day", start_date=datetime(2023,1,1), schedule_interval="@daily", catchup=False) as dag:
    start = DummyOperator(task_id='start')

    end = DummyOperator(task_id='end')

    upload_data_into_fact_price_day = SparkSubmitOperator(
        task_id="upload_data_into_fact_price_day",
        application="/usr/local/spark/app/_upload_data_into_fact_price_day.py", # Spark application path created in airflow and spark cluster
        name=spark_app_name,
        conn_id="spark_default",
        verbose=1,
        conf={"spark.master":spark_master},
        jars="/usr/local/spark/resources/jars/aws-java-sdk-bundle-1.11.972.jar,/usr/local/spark/resources/jars/hadoop-aws-3.3.1.jar",
        application_args=[bucket_name, fact_price_day_path, fact_price_minute_path, ACCESS_KEY, SECRET_KEY, ENDPOINT]
    )

    start >> upload_data_into_fact_price_day >> end
