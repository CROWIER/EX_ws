# Вывод сообщений со временем, когда они былт добавлены
from datetime import datetime
from kafka import KafkaConsumer
import json
from kafka.admin import KafkaAdminClient, NewTopic
import os
from dotenv import load_dotenv
load_dotenv('.env')
key = os.getenv('b_servers')
topic = 'dev4_crowler_html_1'
consumer = KafkaConsumer(topic,
                         security_protocol="SSL",
                         bootstrap_servers=key, api_version=(2, 8, 1),
                         auto_offset_reset='earliest',
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')))
for msg in consumer:
    if msg.offset % 5000 == 0:
        timestamp_string = msg.timestamp
        datetime_object = datetime.fromtimestamp(timestamp_string / 1000)
        print("Msg datetime object:", datetime_object)
    if msg.timestamp < 1672909200000:
        continue

    timestamp_string = msg.timestamp
    datetime_object = datetime.fromtimestamp(timestamp_string/1000)

    # printing resultant datetime object
    print("Msg datetime object:", datetime_object)