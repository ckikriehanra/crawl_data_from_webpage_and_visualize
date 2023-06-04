import datetime
from selenium import webdriver
from bs4 import BeautifulSoup
import time
import csv
from minio import Minio
import pandas as pd
import re
import numpy as np
import boto3

import my_lib.config as config

BRONZE_LAYER_PATH = config.BRONZE_LAYER_PATH
SILVER_LAYER_PATH = config.SILVER_LAYER_PATH
GOLD_LAYER_PATH = config.GOLD_LAYER_PATH
RAW_PATH = config.RAW_PATH
CLEAN_PATH = config.CLEAN_PATH
STAGE_PATH = config.STAGE_PATH

ACCESS_KEY = config.accessKey
SECRET_KEY = config.secretKey
ENDPOINT = config.endpoint

bucketName = config.bucketName
flatFileSilver = config.flatFileSilver
dimSymbolFile = config.dimSymbolFile
factMinutePrice = config.factMinutePrice
factHourPrice = config.factHourPrice
factDayPrice = config.factDayPrice


tempFile = config.tempFile
tempFileParquet = config.tempFileParquet

start_page = config.start_page
end_page = config.end_page

def _crawl_data(ti):
    if end_page <= start_page:
        print("Need end page greater than start page.")
        return

    now = datetime.datetime.now() + datetime.timedelta(hours=7)
    timestr = now.strftime("%Y%m%d-%H%M%S")
    timestr2 = now.strftime("%Y:%m:%d-%H:%M:%S")
    FILE_NAME = timestr + ".csv"

    # Crawl
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--headless")

    driver = webdriver.Remote(
                command_executor='http://chrome-selenium:4444/wd/hub',
                options=chrome_options
            )

    with open(RAW_PATH+FILE_NAME, 'w', encoding='utf8', newline='') as csvfile:
        csvwriter = csv.writer(csvfile) 
        # csvwriter.writerow(['time', 'symbol', 'name','price', '1h%', '24h%', '7d%', "market_cap", 'volume_24h', 'circulating_supply'])
        for page_number in range(start_page, end_page):
            driver.get("https://coinmarketcap.com/?page={}".format(page_number))
            base = 807
            delay = 0.5
            for i in range(1, 13):
                driver.execute_script("window.scrollTo(0, {});".format(base*i))
                time.sleep(delay)

            html_text = driver.page_source
            soup = BeautifulSoup(html_text, "lxml")   

            table = soup.find('table', class_="sc-beb003d5-3 ieTeVa cmc-table")
            coins = table.tbody.find_all('tr')
            for coin in coins:
                attrs = coin.find_all('td')

                symbol = attrs[2].find('p', class_='sc-4984dd93-0 iqdbQL coin-item-symbol').text.lower()
                name = attrs[2].find('p', class_='sc-4984dd93-0 kKpPOn').text.lower()
                price = attrs[3].find('a', class_='cmc-link').text
                index_1h_percentage = attrs[4].find('span').text
                index_24h_percentage = attrs[5].find('span').text
                index_7d_percentage = attrs[6].find('span').text
                try:
                    market_cap = attrs[7].find_all('span')[1].text
                except:
                    market_cap = '$'
                try:
                    volume_24h = attrs[8].find('p').text
                except:
                    volume_24h = '$'
                try:
                    circulating_supply = attrs[9].find('p').text
                except:
                    circulating_supply = '$'
                # Write data into a csv file
                csvwriter.writerow([timestr2, symbol, name, price, index_1h_percentage, index_24h_percentage, index_7d_percentage, 
                                        market_cap, volume_24h, circulating_supply])
    driver.quit()
    ti.xcom_push(key='file_name', value=FILE_NAME)

 
def _upload_data_into_bronze_layer(ti):
    file_name = ti.xcom_pull(key="file_name", task_ids=["crawl_data"])[0]
    year = re.findall(r"([0-9]{4})[0-9]{2}[0-9]{2}-", file_name)[0]
    month = re.findall(r"[0-9]{4}([0-9]{2})[0-9]{2}-", file_name)[0]
    day = re.findall(r"[0-9]{4}[0-9]{2}([0-9]{2})-", file_name)[0]

    # Create a client
    local = Minio(endpoint=re.findall(r'\/\/([0-9a-z:]*)', ENDPOINT)[0],
                  access_key=ACCESS_KEY,
                  secret_key=SECRET_KEY,
                  secure=False)

    # upload data into bronze layer on minio
    bucket_name = bucketName
    object_name = "{}{}/{}/{}/{}".format(BRONZE_LAYER_PATH, year, month, day, re.findall(r"-([\S]*)", file_name)[0])

    file_path = CLEAN_PATH + file_name
    local.fput_object(bucket_name, object_name, file_path)
    ti.xcom_push(key="object_name", value=object_name)

def _preprocessing_data(ti): 
    file_name = ti.xcom_pull(key="file_name", task_ids=["crawl_data"])[0]
    source_file_path = RAW_PATH + file_name
    dest_file_path = CLEAN_PATH + file_name
    col_names = ['time', 'symbol', 'name','price', '1h%', '24h%', '7d%', "market_cap", 'volume_24h', 'circulating_supply']
    raw_data = pd.read_csv(source_file_path, names=col_names, header=None)
    clean_data = pd.DataFrame()
    clean_data['time'] = raw_data['time']
    clean_data['symbol'] = raw_data['symbol']
    clean_data['name'] = raw_data['name']
    
    # remove unwanted character
    clean_data['price'] = raw_data['price'].apply(lambda x: (re.findall(r'\$([0-9]*.*)', x)[0]).replace(',', '').replace('…', '000000000').
                                                                replace('...', '000000000'))
    clean_data['1h%'] = raw_data['1h%'].apply(lambda x: x.replace('%', ''))
    clean_data['24h%'] = raw_data['24h%'].apply(lambda x: x.replace('%', ''))
    clean_data['7d%'] = raw_data['7d%'].apply(lambda x: x.replace('%', ''))
    clean_data['market_cap'] = raw_data['market_cap'].apply(lambda x: (re.findall(r'\$([0-9]*.*)', x)[0]).replace('$', '').replace(',', ''))
    clean_data['volume_24h'] = raw_data['volume_24h'].apply(lambda x: (re.findall(r'\$([0-9]*.*)', x)[0]).replace('$', '').replace(',', ''))
    clean_data['circulating_supply'] = raw_data['circulating_supply'].apply(lambda x: re.findall(r'[0-9]*', x.replace(',', ''))[0])

    # convert from string into float
    clean_data = clean_data.replace(r'', np.NAN, regex=True)
    clean_data['price'] = clean_data['price'].astype('float')
    clean_data['1h%'] = clean_data['1h%'].astype('float')
    clean_data['24h%'] = clean_data['24h%'].astype('float')
    clean_data['7d%'] = clean_data['7d%'].astype('float')
    clean_data['market_cap'] = clean_data['market_cap'].astype('float')
    clean_data['volume_24h'] = clean_data['volume_24h'].astype('float')
    clean_data['circulating_supply'] = clean_data['circulating_supply'].astype('float')

    # convert from string to timestamp 
    clean_data['time'] = pd.to_datetime(clean_data['time'], format='%Y:%m:%d-%H:%M:%S')

    # print(clean_data[['time', 'symbol', 'name', 'price', '1h%', '24h%', '7d%', "market_cap", 'volume_24h', 'circulating_supply']])
    clean_data.to_csv(dest_file_path, index=False)

def _get_latest_file_in_silver_layer(ti):
    s3 = boto3.client('s3',
                  aws_access_key_id=ACCESS_KEY,
                  aws_secret_access_key=SECRET_KEY,
                  endpoint_url=ENDPOINT)

    bucket_name = bucketName
    # prefix = "silver/delta_table/price_coins_follow_minutes"
    prefix = SILVER_LAYER_PATH + flatFileSilver
    objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    # Sort objects by last modified date in descending order
    objects = sorted(objects['Contents'], key=lambda obj: obj['LastModified'], reverse=True)


    # Get the newest object
    newest_object = objects[1]
    newest_object_key = newest_object['Key']
    # newest_object_last_modified = newest_object['LastModified']

    ti.xcom_push(key="file_name_in_parquet", value=newest_object_key)

def _get_latest_file_in_gold_layer_unit(file_name):
    s3 = boto3.client('s3',
                  aws_access_key_id=ACCESS_KEY,
                  aws_secret_access_key=SECRET_KEY,
                  endpoint_url=ENDPOINT)

    bucket_name = bucketName
    
    # Fact_minute_price
    prefix = GOLD_LAYER_PATH + file_name
    objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    # Sort objects by last modified date in descending order
    objects = sorted(objects['Contents'], key=lambda obj: obj['LastModified'], reverse=True)


    # Get the newest object
    newest_object = objects[1]
    return newest_object['Key']

def _get_latest_file_in_gold_layer(ti):
    newest_fact_minute_price = _get_latest_file_in_gold_layer_unit(factMinutePrice)
    newest_fact_hour_price = _get_latest_file_in_gold_layer_unit(factHourPrice)
    newest_fact_day_price = _get_latest_file_in_gold_layer_unit(factDayPrice)
    newest_object_key = [newest_fact_minute_price, newest_fact_hour_price, newest_fact_day_price]

    ti.xcom_push(key="newest_part_in_fact_minute_price_in_gold_layer", value=newest_object_key)





