from kafka import KafkaProducer
import logging
import sys
import os
import requests
import json
from datetime import datetime
from weather_client.transformer import Transformer

import logging
sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/../shared'))

from config import *
logging.basicConfig(level=logging.DEBUG)
WEATHER_API_BASE_ENDPOINT_URL_BY_CITY = 'https://api.openweathermap.org/data/2.5/weather?id='
REQUEST_HEADERS = {"content-type": "application/json"}
WEATHER_API_TOKEN = os.environ.get('WEATHER_API_TOKEN')


class WeatherClient(object):
    _instance = None
    def __new__(cls, *args, **kwards):
        if not isinstance(cls._instance, cls):
            cls._instance = object.__new__(cls, *args, **kwards)
        return cls._instance

    def __init__(self):
        logging.info("initial creation ------")
        self.last_lookup_at = {}
        self._producer = KafkaProducer(bootstrap_servers=[BOOTSTRAP_SERVER])

    def setlast_lookup_at(self, city_id, lookup_at):
        self.last_lookup_at[city_id] = lookup_at

    def getlast_lookup_at(self, city_id):
        return self.last_lookup_at[city_id]

    def _call_weather_api(self, city_id):
        url = WEATHER_API_BASE_ENDPOINT_URL_BY_CITY + str(city_id) + "&APPID=" + WEATHER_API_TOKEN
        r = requests.get(url, headers=REQUEST_HEADERS)
        data = r.json()
        return data

    def request(self):
        for city_id in CITY_IDS:

            data = self._call_weather_api(city_id)
            logging.critical(data)

            transformer = Transformer(data)
            transformed_record = transformer.run()

            if city_id not in self.last_lookup_at:
                self.last_lookup_at[city_id] = {}

            if transformed_record['dt'] != self.last_lookup_at[city_id]:
                msg =  json.dumps(transformed_record).encode("utf-8")
                logging.critical(msg)
                logging.critical("transformed_record")

                self._producer.send(STREAMING_DATA_TOPIC_NAME, msg)
                self.last_lookup_at[city_id] = transformed_record['dt']
                logging.info("send_record city_id:{} time:#{}".format(city_id, transformed_record['dt']))

    def _load_kafka_producer(self):
        producer = None
        while producer is None:
            try:
                producer = KafkaProducer(bootstrap_servers=[BOOTSTRAP_SERVER])
                return producer
            except Exception as e:
                logging.error(e)
                #sleep(10)
