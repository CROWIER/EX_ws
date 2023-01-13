# Запись данных на 1 тайтл
import json
from datetime import datetime
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
import psycopg2
from dotenv import load_dotenv
from kafka import KafkaConsumer, TopicPartition, KafkaProducer
from selenium import webdriver
import requests
import sys
import os


def is_valid(url):
    """
    Checks whether `url` is a valid URL.
    """
    parsed = urlparse(url)
    return bool(parsed.netloc) and bool(parsed.scheme)


load_dotenv('.env')
key = os.getenv('b_servers')
connection = psycopg2.connect(
    host="expertonica-dev-rds.cmmavsb2ijcp.eu-north-1.rds.amazonaws.com",
    database="okkam_db",
    user="okkam_admin",
    password="xwzKdwhNrQf1LfVß#")
TOPIC = 'dev{}_crowler_html_{}'.format(sys.argv[1], sys.argv[2])
consumer = KafkaConsumer(TOPIC,
                         security_protocol="SSL",
                         bootstrap_servers=key,
                         api_version=(2, 8, 1),
                         auto_offset_reset='earliest',
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')))


def get_count_from_db(q, v):
    try:
        cur = connection.cursor()
        cur.execute(q, v)
        res = cur.fetchone()
        # print(type(res), res)
        return res
    except Exception as e:
        print(e)


# for msg in consumer:
#       if msg.offset > 5219:
#          print(msg.offset)
for msg in consumer:
    # print(msg)
    # if msg.offset < int(sys.argv[2]):
    #     if msg.offset % 1000 == 0:
    #         print(msg.offset, msg.value['url'], msg.value['doc_id'])
    # # print([str(msg.value['html']), str(msg.value['url']), str(msg.value['current_url']),
    # #                int(msg.value['doc_id']), int(msg.value['site_id']), int(msg.value['level']), str(msg.value['driver']),
    # #             str(asd.scheme), str(asd.netloc), str(asd.path), str(asd.params), str(asd.query), str(asd.fragment)])
    try:
        # if msg.offset % 1000 == 0:
        #     print(msg.offset, msg.value['url'], msg.value['doc_id'], msg.value['site_id'])
        asd = urlparse(msg.value['current_url'])

        soup = BeautifulSoup(msg.value['html'], 'html.parser')
        for title in soup.find_all('title'): #Пробежка по тайтлам страницы
            # print(title)
            # cursor = connection.cursor()
            # cursor.execute("SELECT count(*) FROM UT2 WHERE title = %s AND url = %s", [str(title.get_text()), str(msg.value['current_url'])])
            # data = cursor.fetchone()[0]
            # print("count url:", data)
            # if data != 0:
            #    continue
            # cursor.execute('''INSERT INTO UT2(title, url) VALUES( %s, %s)''',
            #               [str(title.get_text()), str(msg.value['current_url'])])
            domain_name = urlparse(msg.value['url'])
            urlsToInsert = [domain_name, msg.value['url']]
            urls = set()
            internal_urls = set()
            for a_tag in soup.findAll("a"):
                href = a_tag.attrs.get("href")

                text = (a_tag.get_text())

                if href == "" or href is None:
                    # href empty tag
                    continue
                if (href[0] == "#"):
                    continue
                if (href[0] == "/"):
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
                # cursor = connection.cursor()
                # cursor.execute("SELECT count(*) FROM UA WHERE a_href = %s AND text = %s",
                #               [str(href), str(text)])
                # data = cursor.fetchone()[0]
                # print("count url:", data)
                # if data != 0:
                #    continue
                # cursor.execute('''INSERT INTO UA(a_href, text) VALUES( %s, %s)''',
                #               [str(href), str(text)])
                urls.add(href)
                internal_urls.add(href)

                # cursor = connection.cursor() cursor.execute("SELECT count(*) FROM html_db6 WHERE title = %s AND url
                # = %s AND a_href = %s AND text = %s", [str(title.get_text()), str(msg.value['current_url']),
                # str(href), str(text)]) #(title TEXT, url TEXT, href TEXT, h_text TEXT, current_url TEXT,
                # doc_id TEXT, site_id TEXT, level TEXT); data = cursor.fetchone()[0]
                #Проверка на повторение
                data = get_count_from_db(
                    "SELECT count(*) FROM html_db6 WHERE title = %s AND current_url = %s AND href = %s AND h_text = %s AND url = %s AND level = %s",
                    [str(title.get_text()), str(msg.value['current_url']), str(href), str(text), str(msg.value['url']), str(msg.value['level'])])
                if data[0] != 0:
                    continue
                # (title TEXT, url TEXT, href TEXT, h_text TEXT, current_url TEXT, doc_id TEXT, site_id TEXT,
                # level TEXT) cursor.execute('''INSERT INTO html_db6(title, url, href, h_text, current_url, doc_id,
                # site_id, level) VALUES(%s, %s, %s, %s, %s, %s, %s, %s)''', [str(title.get_text()), str(msg.value[
                # 'url']), str(href), str(text), str(msg.value['current_url']), str(msg.value['doc_id']),
                # str(msg.value['site_id']), int(msg.value['level'])]) print("ADa")
                cur = connection.cursor()
                now = datetime.now()
                timestamp = datetime.timestamp(now)
                #Запись в бд
                cur.execute('''INSERT INTO html_db6(title, url, href, h_text, current_url, doc_id, site_id, level, date) 
                VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)''',
                            [str(title.get_text()), str(msg.value['url']), str(href), str(text),
                             str(msg.value['current_url']), str(msg.value['doc_id']), str(msg.value['site_id']),
                             str(msg.value['level']), now])
                connection.commit()
                # cur.close()
    except Exception as e:
        print(msg.offset, asd, e)
