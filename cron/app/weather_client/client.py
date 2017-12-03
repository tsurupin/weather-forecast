from kafka import KafkaProducer
#from .settings import BOOTSTRAP_SERVER, TOPIC_NAME
import logging
import os
from datetime import datetime
import requests
import json
from weather_client.transformer import Transformer
logging.basicConfig(level=logging.DEBUG)


BOOTSTRAP_SERVER = "172.17.0.1"

TOPIC_NAME = "test"
WEATHER_API_BASE_ENDPOINT_URL_BY_CITY = 'https://api.openweathermap.org/data/2.5/weather?id='
REQUEST_HEADERS = {"content-type": "application/json"}
WEATHER_API_TOKEN = os.environ.get('WEATHER_API_TOKEN')
CITY_IDS = [1689973]

class WeatherClient(object):
    _instance = None
    def __new__(cls, *args, **keys):
        if cls.__instance is None:
            cls.__instance = object.__new__(cls)
        return cls.__instance

    def __init__(self):
        self.__last_lookup_at = {}
        self.__producer = KafkaProducer(bootstrap_servers=[BOOTSTRAP_SERVER])

    def set_last_lookup_at(self, city_id, lookup_at):
        self.__last_lookup_at[city_id] = lookup_at

    def get_last_lookup_at(self, city_id):
        return self.__last_lookup_at[city_id]

    def request(self, city_id):
        url = WEATHER_API_BASE_ENDPOINT_URL_BY_CITY + str(city_id) + "&APPID=" + WEATHER_API_TOKEN
        r = requests.get(url, headers=REQUEST_HEADERS)
        data = r.json()
        logging.info("request, city_id" + str(city_id))
        return data

    def run(self):
        logging.critical("----------start processing-------")
        for city_id in CITY_IDS:

            data = request(city_id)
            transformer = Transformer(data)
            transformed_record = transformer.run()
            if transformed_record['measured_at'] != self.__last_lookup_at[city_id]:
                msg =  json.dumps(transformed_record).encode("utf-8")
                self.__producer.send(TOPIC_NAME, msg)
                self.__last_lookup_at[city_id] = transformed_record['measured_at']
                logging.info("send_record city_id:{} time:#{}".format(city_id, transformed_record['measured_at']))

        logging.critical("----------end processing-------")




