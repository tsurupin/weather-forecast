from kafka import KafkaProducer
#from .settings import BOOTSTRAP_SERVER, TOPIC_NAME
import logging
import sys
import os
import requests
import json
from datetime import datetime
from weather_client.transformer import Transformer
logging.basicConfig(level=logging.DEBUG)

import logging
logging.basicConfig(level=logging.DEBUG)
sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/../shared'))

from config import *

WEATHER_API_BASE_ENDPOINT_URL_BY_CITY = 'https://api.openweathermap.org/data/2.5/weather?id='
REQUEST_HEADERS = {"content-type": "application/json"}
WEATHER_API_TOKEN = os.environ.get('WEATHER_API_TOKEN')
SANFRANCISCO_ID = 1689973
CITY_IDS = [SANFRANCISCO_ID]

class WeatherClient(object):
    _instance = None
    def __new__(cls, *args, **kwards):
        if not isinstance(cls._instance, cls):
            cls._instance = object.__new__(cls, *args, **kwards)
        return cls._instance

    def __init__(self):
        logging.info("initial creation ------")
        self._last_lookup_at = {}
        self._producer = KafkaProducer(bootstrap_servers=[BOOTSTRAP_SERVER])

    def set_last_lookup_at(self, city_id, lookup_at):
        self._last_lookup_at[city_id] = lookup_at

    def get_last_lookup_at(self, city_id):
        return self._last_lookup_at[city_id]

    def _call_weather_api(self, city_id):
        url = WEATHER_API_BASE_ENDPOINT_URL_BY_CITY + str(city_id) + "&APPID=" + WEATHER_API_TOKEN
        r = requests.get(url, headers=REQUEST_HEADERS)
        data = r.json()
        logging.info("request, city_id" + str(city_id))
        logging.info(data)
        return data

    def request(self):
#         cron_1       | INFO:root:{'coord': {'lon': 120.83, 'lat': 15.35}, 'cod': 200, 'main': {'temp_max': 300.15, 'humidity': 65, 'temp_min': 300.15, 'pressure': 1016, 'temp': 300.15}, 'visibility': 10000, 'weather': [{'main': 'Clouds', 'description': 'broken clouds', 'id': 803, 'icon': '04d'}], 'sys': {'type': 1, 'country': 'PH', 'id': 7704, 'sunrise': 1514586136, 'sunset': 1514626569, 'message': 0.023}, 'dt': 1514599200, 'clouds': {'all': 75}, 'name': 'San Francisco', 'base': 'stations', 'id': 1689973, 'wind': {'speed': 3.1, 'deg': 330}}
# cron_1       | CRITICAL:root:api_data: {'coord': {'lon': 120.83, 'lat': 15.35}, 'cod': 200, 'main': {'temp_max': 300.15, 'humidity': 65, 'temp_min': 300.15, 'pressure': 1016, 'temp': 300.15}, 'visibility': 10000, 'weather': [{'main': 'Clouds', 'description': 'broken clouds', 'id': 803, 'icon': '04d'}], 'sys': {'type': 1, 'country': 'PH', 'id': 7704, 'sunrise': 1514586136, 'sunset': 1514626569, 'message': 0.023}, 'dt': 1514599200, 'clouds': {'all': 75}, 'name': 'San Francisco', 'base': 'stations', 'id': 1689973, 'wind': {'speed': 3.1, 'deg': 330}}
# cron_1       | CRITICAL:root:transformed_record: {'sunset': 1514626569, 'temperature': 300.15, 'sunrise': 1514586136, 'condition': 'Clouds', 'humidity': 65, 'wind_degree': 330, 'wind_speed': 3.1, 'pressure': 1016, 'measured_at': 1514599200, 'country_code': 'PH', 'clouds_all': 75, 'latitude': 15.35, 'city': 'San Francisco', 'longitude': 120.83, 'condition_details': 'broken clouds'}
        logging.critical("----------start processing-------")
        for city_id in CITY_IDS:

            data = self._call_weather_api(city_id)
            logging.critical('api_data: {}'.format(data))

            transformer = Transformer(data)
            transformed_record = transformer.run()
            logging.critical('transformed_record: {}'.format(transformed_record))
            if city_id not in self._last_lookup_at:
                self._last_lookup_at[city_id] = {}

            if transformed_record['measured_at'] != self._last_lookup_at[city_id]:
                msg =  json.dumps(transformed_record).encode("utf-8")
                self._producer.send(STREAMING_DATA_TOPIC_NAME, msg)
                self._last_lookup_at[city_id] = transformed_record['measured_at']
                logging.info("send_record city_id:{} time:#{}".format(city_id, transformed_record['measured_at']))

        # transformed_record = {"condition_details": "scattered clouds", "city": "San Francisco", "latitude": 15.35, "pressure": 1011, "country_code": "PH", "wind_speed": 3.6, "temperature": 299.15, "sunrise": 1512425382, "longitude": 120.83, "condition": "Clouds", "sunset": 1512465917, "wind_degree": 320, "humidity": 78, "measured_at": 1512489600, "clouds_all": 40}
        # msg =  json.dumps(transformed_record).encode("utf-8")
        # self._producer.send(TOPIC_NAME, msg)
        # #self._last_lookup_at[city_id] = transformed_record['measured_at']
        # logging.critical("----------end processing-------")
