
import os
# os.environ['PYSPARK_SUBMIT_ARGS'] = '\
# --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0,com.datastax.spark:spark-cassandra-connector_2.11:2.0.1 \
# pyspark-shell'
#
# from pyspark import SparkConf, SparkContext
# from pyspark.streaming import StreamingContext
# from pyspark.streaming.kafka import KafkaUtils
#
# from pyspark.sql import SQLContext, SparkSession, Row
# from pyspark.sql.types import *
import os
import sys
import uuid
import logging
import datetime
import json
from cassandra.cluster import Cluster
from cassandra.query import ordered_dict_factory
from kafka import KafkaConsumer

from time import sleep
import logging
#logging.basicConfig(level=logging.DEBUG)
sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/shared'))
from config import *
sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/shared/predictions'))
from forecast import Forecast

WAIT_TIME_IN_SECOND = 60

class Streaming(object):

    def run(self):
        sleep(30)

        consumer = self._load_kafka_consumer()
        self._consume_message(consumer)

        consumer.close()

    def _consume_message(self, consumer):
        for msg in consumer:
            self._save(msg.value)
            self._predict_weather()


    def _save(self, data):

        session = self._load_cassandra_session()

        session.row_factory = ordered_dict_factory
        unix_datetime = None if data.get('dt') is None else datetime.datetime.fromtimestamp(data.get('dt'))
        session.execute('''
        INSERT INTO {} (id, dt, dt_iso, measured_at, clouds_all, condition_id, condition_details, condition, city_name, city_id, temperature, temperature_max, temperature_min, rain_3h, snow_3h, wind_speed, wind_degree, humidity, pressure)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''.format(RAW_DATA_TABLE_NAME), (
                str(uuid.uuid4()),
                data.get('dt'),
                unix_datetime,
                unix_datetime,
                data.get('clouds_all'),
                data.get('condition_id'),
                data.get('condition_details'),
                data.get('condition'),
                data.get('city_name'),
                data.get('city_id'),
                data.get('temperature'),
                data.get('temperature_max'),
                data.get('temperature_min'),
                data.get('rain_3h'),
                data.get('snow_3h'),
                data.get('wind_speed'),
                data.get('wind_degree'),
                data.get('humidity'),
                data.get('pressure')
            )
        )
        logging.critical("save is done!--------------")

    def _predict_weather(self):
        from forecast import Forecast
        forecast = Forecast(type="streaming")
        forecast.preprocess()
        forecast.fit()
        prediction = forecast.predict()
        logging.critical('result: {}'.format(prediction))
        forecast.save()

    def _load_kafka_consumer(self):
        consumer = None
        while consumer is None:
            try:
                consumer = KafkaConsumer(
                    STREAMING_DATA_TOPIC_NAME,
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                    bootstrap_servers=[BOOTSTRAP_SERVER]
                )
                return consumer
            except Exception as e:
                logging.critical(e)
                sleep(5)

    def _load_cassandra_session(self):
        session = None
        while session is None:
            try:
                cluster = Cluster([os.environ.get('CASSANDRA_PORT_9042_TCP_ADDR', 'localhost')],
                                  port=int(os.environ.get('CASSANDRA_PORT_9042_TCP_PORT', 9042))
                                  )
                session = cluster.connect(KEYSPACE_NAME)
                return session
            except Exception as e:
                logging.error(e)
                sleep(5)


if __name__ == '__main__':
    streaming = Streaming()
    streaming.run()
