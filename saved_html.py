import sqlite3
from time import sleep
import sys
from urllib.parse import urlparse

import psycopg2 as psycopg2
import requests as requests
from kafka import KafkaConsumer, KafkaProducer
import json
from dotenv import load_dotenv
from kafka import KafkaConsumer
import os
connection = psycopg2.connect(
    host="expertonica-dev-rds.cmmavsb2ijcp.eu-north-1.rds.amazonaws.com",
    database = "okkam_db",
    user="okkam_admin",
    password="xwzKdwhNrQf1LfV#")


load_dotenv('.env')
key = os.getenv('b_servers')
print("level:", sys.argv[1], sys.argv[2])
consumer = KafkaConsumer('dev4_crowler_html_{}'.format(sys.argv[1]),
                             security_protocol="SSL",
                             bootstrap_servers=key, api_version=(2, 8, 1),
                             auto_offset_reset='earliest',
                             value_deserializer=lambda m: json.loads(m.decode('utf-8')))

stack = []
for msg in consumer:
    print(len(stack), msg.value['url'], msg.value['doc_id'])
    print(msg.value['url'])
    stack.append(msg.value['url'])
    if len(stack) > 10:
        cursor = connection.cursor()
        for one in stack:
            domain_name = urlparse(msg.value['url'])
            urlsToInsert = [domain_name, one]
            cursor.execute('''INSERT INTO a_links(domain, url) VALUES(%s, %s)''', urlsToInsert)
            connection.commit()
        # connection.close()
        # connection.close()
            # print("Соединение с SQLite закрыто")
        stack = []

    # producer = KafkaProducer(
    #     security_protocol="SSL",
    #     bootstrap_servers=key,
    #     api_version=(2, 8, 1),
    #     value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    #
    # msg_for_send = {'url': msg.value['url'],
    #                 'doc_id': msg.value['doc_id'], 'site_id': msg.value['site_id'],
    #                 'level': msg.value['level']}
    # jsonString = json.dumps(msg_for_send, indent=4)
    # msg1 = producer.send('dev4_crawler_result_{}'.format(msg.value['level']),
    #                      json.loads(jsonString.encode('utf-8')))
    # producer.flush()
