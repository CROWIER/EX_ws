from urllib.parse import urljoin, urlparse
import sys
import requests as requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver import ActionChains
from urllib.parse import urlparse, urljoin, quote
from kafka import KafkaConsumer, KafkaProducer
from bs4 import BeautifulSoup
import json
from dotenv import load_dotenv
from kafka import KafkaConsumer
import os

load_dotenv('.env')
key = os.getenv('b_servers')
if 'request' in sys.argv[1]:
    topic = 'dev4_req_crowler_start'
else:
    topic = 'dev4_crowler_start'
consumer = KafkaConsumer(topic,
                         security_protocol="SSL",
                         bootstrap_servers=key, api_version=(2, 8, 1),
                         auto_offset_reset='earliest',
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')))

options = Options()
options.headless = True

# Активизация webdriver
# driver = webdriver.Chrome('C:\Chromedriver\chromedriver.exe', options=options)

# Вызов сайта
# start_url = 'https://kaspi.kz/shop/'
urls = set()
if 'request' not in sys.argv[1]:
    driver = webdriver.Chrome('chromedriver')
    for msg in consumer:
        print(msg)
        print(msg.value['url'])
        driver.get(msg.value['url'])


        # Для дальнейшего сохранения файла без ошибок из-за неразрешенных символов
        def file_safe_url(url):
            return driver.current_url.replace('https://', '').replace('/', '-').replace(':', '').replace('?', '').replace(
                '*', '')



        producer = KafkaProducer(
            security_protocol="SSL",
            bootstrap_servers=key,
            api_version=(2, 8, 1),
            value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        #Создание JSON со всеми данными
        dictionary = {'html': driver.page_source, 'url': msg.value['url'], 'current_url': driver.current_url,
                      'doc_id': msg.value['DOC_ID'], 'site_id': msg.value['SITE_ID'],
                      'level': 0, 'driver': 'Selenium'}
        jsonString = json.dumps(dictionary, indent=4)
        msg1 = producer.send('dev4_crowler_html_0', json.loads(jsonString.encode('utf-8'))) #отправка JSON
        producer.flush()
else:
    for msg in consumer:
        print(msg)
        print(msg.value['url'])
        requests.get(msg.value['url'])
        response = requests.get(msg.value['url'])


        # Для дальнейшего сохранения файла без ошибок из-за неразрешенных символов
        def file_safe_url(url):
            return driver.current_url.replace('https://', '').replace('/', '-').replace(':', '').replace('?',
                                                                                                         '').replace(
                '*', '')



        producer = KafkaProducer(
            security_protocol="SSL",
            bootstrap_servers=key,
            api_version=(2, 8, 1),
            value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        # Создание JSON со всеми данными
        dictionary = {'html': response.content, 'url': msg.value['url'], 'current_url': response.url,
                      'doc_id': msg.value['DOC_ID'], 'site_id': msg.value['SITE_ID'],
                      'level': 0, 'driver': 'Selenium'}
        jsonString = json.dumps(dictionary, indent=4)
        msg1 = producer.send('dev4_req_crowler_html_0', json.loads(jsonString.encode('utf-8'))) # Отправка JSON
        producer.flush()
