import os
import sqlite3
from time import sleep
from urllib.parse import urljoin, urlparse

import psycopg2
import requests
import json
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer
import sys

sleep(1)


# LEVEL = sys.argv[1]
# if LEVEL == 0:
#     LEVEL = ''

def is_valid(url):
    """
    Checks whether `url` is a valid URL.
    """
    parsed = urlparse(url)
    return bool(parsed.netloc) and bool(parsed.scheme)


load_dotenv('.env')
key = os.getenv('b_servers')
print(sys.argv[1], sys.argv[2])
connection = psycopg2.connect("dbname=a_links user=okkam_admin password=xwzKdwhNrQf1LfV#") # Подключение к БД

cursor = connection.cursor()
if 'request' in sys.argv[2]:
    top = 'dev4_req_crowler_html_{}'.format(sys.argv[1])
else:
    top = 'dev4_crowler_html_{}'.format(sys.argv[1])
# Получение JSON из топика
consumer = KafkaConsumer(top,
                         security_protocol="SSL",
                         bootstrap_servers=key, api_version=(2, 8, 1),
                         auto_offset_reset='earliest',
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')))

# web scraping
def run_job(msg):
    print(msg.value)
    cursor.execute("SELECT count(*) FROM Links WHERE url = ?", (msg.value['url'],)) # Проверка на повторение
    data = cursor.fetchone()[0]
    if data == 0:

        domain_name = urlparse(msg.value['url'])
        urlsToInsert = [domain_name, msg.value['url']]
        cursor.execute('''INSERT INTO a_links(domain, url) VALUES(?, ?)''', urlsToInsert) # Запись
        connection.commit()
    else:
        return
    urls = set()
    internal_urls = set()
    soup = BeautifulSoup(msg.value['html'], "html.parser")
    for a_tag in soup.findAll("a"):
        href = a_tag.attrs.get("href")
        if href == "" or href is None:
            # href empty tag
            continue
        if href[0] == "#":
            continue
        if href[0] == "/":
            href = urljoin(msg.value['url'], href)
        parsed_href = urlparse(href)
        # remove URL GET parameters, URL fragments, etc.
        href = parsed_href.scheme + "://" + parsed_href.netloc + parsed_href.path
        if not is_valid(href):
            # not a valid URL
            continue
        if href in internal_urls:
            # already in the set
            continue
        if domain_name.netloc not in href:
            # external link
            # if href not in external_urls:
            #     print(f"{GRAY}[!] External link: {href}{RESET}")
            #     external_urls.add(href)
            continue
        # print(f"{GREEN}[*] Internal link: {href}{RESET}")
        urls.add(href)
        internal_urls.add(href)
    producer = KafkaProducer(
        security_protocol="SSL",
        bootstrap_servers=key,
        api_version=(2, 8, 1),
        value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    for url in urls:
        dictionary = {'referer_url': msg.value['url'],
                      'doc_id': msg.value['doc_id'], 'site_id': msg.value['site_id'],
                      'level': msg.value['level'] + 1, 'driver': 'Selenium', 'url': url}
        jsonString = json.dumps(dictionary, indent=4)
        if 'request' in sys.argv[2]:
            t = 'dev4_req_crowler_level_{}'.format(msg.value['level'] + 1)
        else:
            t = 'dev4_crowler_level_{}'.format(msg.value['level'] + 1)
        msg1 = producer.send(t, json.loads(jsonString.encode('utf-8')),
                             )
        producer.flush()


for msg in consumer:
    while True:
        print(msg.offset, msg.value['doc_id'], msg.value['url'])
        sleep(1)
        try:
            run_job(msg)
        except sqlite3.Error as error:
            print("Ошибка при работе с SQLite", error)
        finally:
            if (connection):
                print("Всего строк, измененных после подключения к базе данных: ", connection.total_changes)
                connection.close()
                print("Соединение с SQLite закрыто")
