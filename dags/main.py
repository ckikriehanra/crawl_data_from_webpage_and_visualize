from airflow import DAG 
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from my_lib.util import _crawl_data, _preprocessing_data, _upload_data_into_bronze_layer, _get_latest_file_in_silver_layer
from datetime import datetime, timedelta
import my_lib.config as config

spark_master = "spark://spark:7077"
spark_app_name1 = "upload-data-into-silver-layer.py"
spark_app_name2 = "upload_data_into_gold_layer.py"

bucket_name = config.bucketName
file_silver_layer_path = config.SILVER_LAYER_PATH + config.flatFileSilver
dim_symbol_path = config.GOLD_LAYER_PATH + "dim/" + config.dimSymbolFile
dim_date_path = config.GOLD_LAYER_PATH + "dim/" + config.dimDateFile
fact_price_minute_path = config.GOLD_LAYER_PATH + "fact/" + config.factPriceMinute

ACCESS_KEY = config.accessKey
SECRET_KEY = config.secretKey
ENDPOINT = config.endpoint


with DAG("crawl_dag", start_date=datetime(2023,1,1), schedule_interval="*/5 * * * *", catchup=False, max_active_runs=1) as dag:
    crawl_data = PythonOperator(
        task_id="crawl_data",
        python_callable=_crawl_data
    )

    preprocessing_data = PythonOperator(
        task_id="preprocessing_data",
        python_callable=_preprocessing_data
    )

    upload_data_into_bronze_layer = PythonOperator(
        task_id="upload_data_into_bronze_layer",
        python_callable=_upload_data_into_bronze_layer
    )

    object_name = "{{ti.xcom_pull(key='object_name', task_ids='upload_data_into_bronze_layer')}}"

    upload_data_into_silver_layer = SparkSubmitOperator(
        task_id="upload_data_into_silver_layer",
        application="/usr/local/spark/app/_upload_data_into_silver_layer.py", # Spark application path created in airflow and spark cluster
        name=spark_app_name1,
        conn_id="spark_default",
        verbose=1,
        conf={"spark.master":spark_master},
        jars="/usr/local/spark/resources/jars/aws-java-sdk-bundle-1.11.972.jar,/usr/local/spark/resources/jars/hadoop-aws-3.3.1.jar",
        application_args=[object_name, file_silver_layer_path, ACCESS_KEY, SECRET_KEY, ENDPOINT]
    )

    get_latest_file_in_silver_layer = PythonOperator(
        task_id="get_latest_file_in_silver_layer",
        python_callable=_get_latest_file_in_silver_layer
    )

    object_name = "{{ti.xcom_pull(key='file_name_in_parquet', task_ids='get_latest_file_in_silver_layer')}}"

    upload_data_into_gold_layer = SparkSubmitOperator(
        task_id="upload_data_into_gold_layer",
        application="/usr/local/spark/app/_upload_data_into_gold_layer.py", # Spark application path created in airflow and spark cluster
        name=spark_app_name2,
        conn_id="spark_default",
        verbose=1,
        conf={"spark.master":spark_master},
        jars="/usr/local/spark/resources/jars/aws-java-sdk-bundle-1.11.972.jar,/usr/local/spark/resources/jars/hadoop-aws-3.3.1.jar",
        application_args=[object_name, bucket_name, dim_symbol_path, dim_date_path, fact_price_minute_path, ACCESS_KEY, SECRET_KEY, ENDPOINT]
    )

    trigger_task = TriggerDagRunOperator(
        task_id='trigger_task',
        trigger_dag_id='load_data_into_data_warehouse'
    )

    crawl_data >> preprocessing_data >> upload_data_into_bronze_layer >> upload_data_into_silver_layer >> get_latest_file_in_silver_layer >> upload_data_into_gold_layer >> trigger_task
