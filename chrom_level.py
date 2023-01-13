import sqlite3
from time import sleep
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
from random import randrange

import psycopg2 as psycopg2
connection = psycopg2.connect(
    host="expertonica-dev-rds.cmmavsb2ijcp.eu-north-1.rds.amazonaws.com",
    database = "okkam_db",
    user="okkam_admin",
    password="xwzKdwhNrQf1LfV#")

sleep(1)
load_dotenv('.env')
key = os.getenv('b_servers')
print("level:", sys.argv[1], sys.argv[2], "count driver:", sys.argv[3], "curent driver:", sys.argv[4], "\nsleeping 15 sec")
sleep(15)
# Активизация webdriver
# driver = webdriver.Chrome('C:\Chromedriver\chromedriver.exe', options=options)
if 'request' in sys.argv[2]:
    topic = 'dev4_req_crowler_level_{}'.format(sys.argv[1])
else:
    topic = 'dev4_crowler_level_{}'.format(sys.argv[1])
    driver = webdriver.Chrome('./chromedriver{}'.format(sys.argv[1]))

    consumer = KafkaConsumer(topic,
                             security_protocol="SSL",
                             bootstrap_servers=key, api_version=(2, 8, 1),
                             auto_offset_reset='earliest',
                             value_deserializer=lambda m: json.loads(m.decode('utf-8')))

    options = Options()
    options.headless = True

    urls = set()

    def run_job(msg):
        print("run_job:", msg.value)
        print(msg.value['url'])
        cursor = connection.cursor()
        cursor.execute('''SELECT count(*) FROM a_links WHERE url = %s''', [msg.value['url']])
        data = cursor.fetchone()[0]
        print("count url:", data)
        if data == 0:
            try:
                domain_name = urlparse(msg.value['url'])
                urlsToInsert = [domain_name, msg.value['url']]

                cursor.execute('''INSERT INTO a_links(domain, url) VALUES(%s, %s)''', urlsToInsert) # Проверка на повторение
                driver.get(msg.value['url']) # Открывание страниц в браузере
                producer = KafkaProducer(
                    security_protocol="SSL",
                    bootstrap_servers=key,
                    api_version=(2, 8, 1),
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'))
                msg_for_send = {'html': driver.page_source, 'url': msg.value['url'], 'current_url': driver.current_url,
                                'doc_id': msg.value['doc_id'], 'site_id': msg.value['site_id'],
                                'level': msg.value['level'], 'driver': 'Selenium'}
                jsonString = json.dumps(msg_for_send, indent=4)
                msg1 = producer.send('dev4_crowler_html_{}'.format(msg.value['level']),
                                     json.loads(jsonString.encode('utf-8')))
                producer.flush()
                connection.commit()
            except:
                return
        else:
             return

    for msg in consumer:
        while True:
            print(msg.offset, msg.value['doc_id'], msg.value['url'])
            if int(msg.offset) % int(sys.argv[3]) == int(sys.argv[4]):
                try:
                    run_job(msg)
                    break
                except Exception as error:
                    print("Ошибка при работе", error)
                sleep(1)
            else:
                break

connection.close()
