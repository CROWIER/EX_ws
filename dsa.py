#Проверочный файл
import json
from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
import os
from dotenv import load_dotenv
load_dotenv('.env')
key = os.getenv('b_servers')
topic = 'dev5_crowler_html_2'
consumer = KafkaConsumer(topic,
                         security_protocol="SSL",
                         bootstrap_servers=key, api_version=(2, 8, 1),
                         auto_offset_reset='earliest',
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')))
st = {}
for msg in consumer:
    if st.get(msg.value['doc_id']):
        st[msg.value['doc_id']] += 1
    else:
        st[msg.value['doc_id']] = 1
    print(msg.offset, st)

