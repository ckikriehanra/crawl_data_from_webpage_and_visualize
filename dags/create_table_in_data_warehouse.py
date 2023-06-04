from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
import psycopg2
from my_lib import config

postgresHost = config.postgresHost
postgresPort = config.postgresPort
postgresDatabaseName = config.postgresDatabaseName
postgresUser = config.postgresUser
postgresPassword = config.postgresPassword

def _create_table():
    conn = psycopg2.connect(
        host=postgresHost,
        port=postgresPort,
        database=postgresDatabaseName,
        user=postgresUser,
        password=postgresPassword
    )

    cursor = conn.cursor()
    create_table_query = '''
        CREATE TABLE IF NOT EXISTS DIM_DATE(Date_key INT,TheDate DATE,TheDay INT,TheDayName VARCHAR(50),TheWeek INT,TheDayOfWeek INT,TheMonth VARCHAR(50),TheMonthName VARCHAR(50),TheQuarter INT,TheYear INT,TheMonthYear VARCHAR(50),TheYearMonth VARCHAR(50),TheQuarterYear VARCHAR(50),TheYearQuater VARCHAR(50),TheFirstOfMonth DATE,TheLastOfYear DATE);
        CREATE TABLE IF NOT EXISTS DIM_SYMBOL(symbol_key INT,symbol VARCHAR(50),name VARCHAR(50));
        CREATE TABLE IF NOT EXISTS FACT_MINUTE_PRICE(date_key INT,symbol_key INT,time TIMESTAMP, price FLOAT, "1h%" FLOAT,"24h%" FLOAT, "7d%" FLOAT, market_cap FLOAT, volume_24h FLOAT, circulating_supply FLOAT );
        CREATE TABLE IF NOT EXISTS FACT_HOUR_PRICE(symbol_key INT,hour INT,day DATE,avg_price_by_hour FLOAT,avg_volume_24h_by_hour FLOAT);
        CREATE TABLE IF NOT EXISTS FACT_DAY_PRICE(symbol_key INT,day DATE,avg_price_by_day FLOAT,avg_volume_24h_by_day FLOAT);
    '''
    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()
    conn.close()

with DAG('create_table_in_data_warehouse', start_date=datetime(2023,1,1), schedule_interval="@once", catchup=False):
    start = DummyOperator(task_id='start')

    create_table = PythonOperator(
        task_id="create_table",
        python_callable=_create_table
    )

    end = DummyOperator(task_id='end')

    start >> create_table >> end
