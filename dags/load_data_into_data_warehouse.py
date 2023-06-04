from airflow import DAG 
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
from my_lib import config
from my_lib.util import _get_latest_file_in_gold_layer

ACCESS_KEY = config.accessKey
SECRET_KEY = config.secretKey
ENDPOINT = config.endpoint

###############################################
# Parameters
###############################################
spark_master = "spark://spark:7077"
spark_app_name = "Load data into data warehouse"

###############################################
# DAG Definition
###############################################

with DAG("load_data_into_data_warehouse", start_date=datetime(2023,1,1), schedule_interval=None, catchup=False) as dag:
    start = BashOperator(
        task_id='start',
        bash_command='sleep 90'
    )

    get_latest_file_in_gold_layer = PythonOperator(
        task_id="get_latest_file_in_gold_layer",
        python_callable=_get_latest_file_in_gold_layer
    )

    object_name = "{{ti.xcom_pull(key='newest_part_in_fact_minute_price_in_gold_layer', task_ids='get_latest_file_in_gold_layer')}}"

    load_data_into_data_warehouse = SparkSubmitOperator(
        task_id="load_data_into_data_warehouse",
        application="/usr/local/spark/app/_load_data_into_data_warehouse.py", # Spark application path created in airflow and spark cluster
        name=spark_app_name,
        conn_id="spark_default",
        verbose=1,
        conf={"spark.master":spark_master},
        jars="/usr/local/spark/resources/jars/aws-java-sdk-bundle-1.11.972.jar,/usr/local/spark/resources/jars/hadoop-aws-3.3.1.jar,/usr/local/spark/resources/jars/postgresql-42.2.24.jar",
        application_args=[ACCESS_KEY, SECRET_KEY, ENDPOINT, object_name]
    )

    end = DummyOperator(task_id='end')


    start >> get_latest_file_in_gold_layer >> load_data_into_data_warehouse >> end
