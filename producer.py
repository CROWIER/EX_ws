# Получение сайтов из excel файла и отправление в очередь
import json
from kafka import KafkaProducer
from dotenv import load_dotenv
import os
import pandas as pd
import sys


def get_http(x):
    x["url"] = "http://" + x['url']
    return x


def get_https(x):
    x['url'] = "https://" + x['url']
    return x


def get_www(x):
    x['url'] = "http://www." + x['url']
    return x


def get_wwws(x):
    x['url'] = "https://www." + x['url']
    return x


df = pd.DataFrame(pd.read_excel('urls.xlsx')) # Создание датафрейма
df.rename(columns={'N': 'DOC_ID'}, inplace=True) # Переименовка N -> DOC_ID
myDf = json.loads(df.to_json())
load_dotenv('.env')
key = os.getenv('b_servers')
if 'request' in sys.argv[1]: # определение топика под request и chromedriver
    topic = 'dev5_req_crowler_start'
else:
    topic = 'dev5_crowler_start'
producer = KafkaProducer( # создание producer
    security_protocol="SSL",
    bootstrap_servers=key,
    api_version=(2, 8, 1),
    value_serializer=lambda v: json.dumps(v).encode('utf-8'))
for i in range(df.shape[0]): # отправка всех ссылок
    myDf1 = get_https(df.loc[i])
    msg1 = producer.send(topic, json.loads(myDf1.to_json().encode('utf-8')))
    myDf2 = get_https(df.loc[i])
    msg2 = producer.send(topic, json.loads(myDf2.to_json().encode('utf-8')))
    myDf3 = get_www(df.loc[i])
    msg3 = producer.send(topic, json.loads(myDf3.to_json().encode('utf-8')))
    myDf4 = get_wwws(df.loc[i])
    msg4 = producer.send(topic, json.loads(myDf4.to_json().encode('utf-8')))
    # break;
    # producer.send('python-kafka1', b'Asd')
producer.flush()
