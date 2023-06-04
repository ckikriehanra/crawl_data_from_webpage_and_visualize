from airflow import DAG 
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
from my_lib import config

ACCESS_KEY = config.accessKey
SECRET_KEY = config.secretKey
ENDPOINT = config.endpoint

###############################################
# Parameters
###############################################
spark_master = "spark://spark:7077"
spark_app_name = "Load data into dim_date"

###############################################
# DAG Definition
###############################################

with DAG("full_load_data_into_dim_date", start_date=datetime(2023,1,1), schedule_interval="@once", catchup=False) as dag:
    start = DummyOperator(task_id='start')

    load_data = SparkSubmitOperator(
        task_id="load_data",
        application="/usr/local/spark/app/_load_data.py", # Spark application path created in airflow and spark cluster
        name=spark_app_name,
        conn_id="spark_default",
        verbose=1,
        conf={"spark.master":spark_master},
        jars="/usr/local/spark/resources/jars/aws-java-sdk-bundle-1.11.972.jar,/usr/local/spark/resources/jars/hadoop-aws-3.3.1.jar",
        application_args=[ACCESS_KEY, SECRET_KEY, ENDPOINT]
    )

    end = DummyOperator(task_id='end')


    # crawl_data >> preprocessing_data >> upload_data_into_bronze_layer >> upload_data_into_silver_layer
    start >> load_data >> end
