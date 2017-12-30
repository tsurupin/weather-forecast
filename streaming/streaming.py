
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
from cassandra.query import ordered_dict_factory, dict_factory
from kafka import KafkaConsumer
#from forecast import Forecast
from time import sleep
import logging
#logging.basicConfig(level=logging.DEBUG)
sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/shared'))

from config import *

WAIT_TIME_IN_SECOND = 60
class Streaming(object):

    def run(self):
        sleep(30)
        #
        # consumer = KafkaConsumer(
        #     STREAMING_DATA_TOPIC_NAME,
        #     value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        #     bootstrap_servers=[BOOTSTRAP_SERVER]
        # )

        while True:
            logging.critical("contineously reading data")
            need_prediction = False
            # for msg in consumer:
            #     logging.critical("finaoyy got message-!!!!!!!!!!!!!!")
            #     logging.critical(msg)
            #     logging.critical(msg.value)
            #
            #     logging.info('streaming_data: {}'.format(msg))
            #     need_prediction = True

            data = {'pressure': 1014, 'sunset': 1514626582, 'city_id': 5391959, 'temperature': 298.15, 'dt': 1514635200, 'country_code': 'PH', 'condition_id': 803, 'longitude': 120.83, 'clouds_all': 75, 'condition': 'Clouds', 'sunrise': 1514586145, 'condition_details': 'broken clouds', 'latitude': 15.35, 'temperature_max': 298.15, 'wind_degree': 50, 'wind_speed': 2.1, 'humidity': 69, 'temperature_min': 298.15, 'city_name': 'San Francisco'}
            self._save(data)
            # if need_prediction:
            #
            #     #self._predict_weather()
            logging.info("load_data-------------")
            sleep(WAIT_TIME_IN_SECOND)

        #consumer.close()

    def _save(self, data):
        cluster = Cluster([os.environ.get('CASSANDRA_PORT_9042_TCP_ADDR', 'localhost')],
                          port=int(os.environ.get('CASSANDRA_PORT_9042_TCP_PORT', 9042))
                          )
        session = None
        logging.critical("before session")
        while session is None:
            try:
                session = cluster.connect(KEYSPACE_NAME)
            except Exception as e:
                logging.error(e)
                sleep(5)
        logging.critical("session------")
        logging.critical(session)
        session.row_factory = dict_factory
        # need to think about uuid
        unix_datetime = None if data.get('dt') is None else datetime.datetime.fromtimestamp(data.get('dt'))
        session.execute('''
        INSERT INTO raw_data (id, dt, dt_iso, measured_at, clouds_all, condition_id, condition_details, condition, city_name, city_id, temperature, temperature_max, temperature_min, rain_3h, snow_3h, wind_speed, wind_degree, humidity, pressure)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', (
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
        forecast.fit()
        prediction = forecast.predict()
        print('result: {}'.format(prediction))
        forecast.save()



    # def _save(self, rdd):
    #     if rdd.count() > 0:
    #         # NOTE: this should be alphabetical order
    #         schema = self._get_schema()
    #
    #         spark = self._get_spark_session_instance(rdd.context.getConf())
    #
    #         named_rdd = rdd.map(self._convert_to_row)
    #         stream_df = spark.createDataFrame(named_rdd, schema)
    #         stream_df.show()
    #         stream_df.write \
    #             .format(CASSANDRA_FORMAT) \
    #             .mode('append') \
    #             .options(table=TABLE_NAME, keyspace=KEY_SPACE) \
    #             .save()

    #
    # def _update_forecast(self, data):
    #
    #     if data.count() is not None:
    #         from forecast import Forecast
    #         logging.critical("-----update_forecast")
    #         forecast = Forecast(type="streaming")
    #         forecast.fit(data)
    #         forecast.predict()
    #
    # def _get_schema(self):
    #     return StructType([
    #         StructField("city", StringType(), True),
    #         StructField("clouds_all", IntegerType()),
    #         StructField("condition", StringType(), True),
    #         StructField("condition_details", StringType(), True),
    #         StructField("humidity", IntegerType(), True),
    #         StructField("latitude", FloatType(), True),
    #         StructField("longitude", FloatType(), True),
    #         StructField("measured_at", TimestampType(), True),
    #         StructField("pressure", IntegerType(), True),
    #         StructField("rain_3h", IntegerType()),
    #         StructField("snow_3h", IntegerType()),
    #         StructField("sunrise", TimestampType(), True),
    #         StructField("sunset", TimestampType(), True),
    #         StructField("temperature", FloatType(), True),
    #         StructField("wind_degree", IntegerType()),
    #         StructField("wind_speed", FloatType())
    #     ])
    #
    # def _get_spark_session_instance(self, spark_conf):
    #     if ("sparkSessionSingletonInstance" not in globals()):
    #         globals()["sparkSessionSingletonInstance"] = SparkSession \
    #             .builder \
    #             .config(conf=spark_conf) \
    #             .getOrCreate()
    #     return globals()["sparkSessionSingletonInstance"]




    # def _convert_to_row(self, c):
    #     # NOTE: this should be alphabetical order
    #     return Row(
    #         city=c.get("city"),
    #         condition=c.get("condition"),
    #         clouds_all=c.get("clouds_all"),
    #         condition_details=c.get("condition_details"),
    #         humidity=c.get("humidity"),
    #         latitude=c.get("latitude"),
    #         longitude=c.get("longitude"),
    #         measured_at=datetime.datetime.fromtimestamp(int(c["measured_at"])),
    #         pressure=c.get("pressure"),
    #         rain_3h=c.get("rain_3h"),
    #         snow_3h=c.get("snow_3h"),
    #         sunrise=datetime.datetime.fromtimestamp(int(c["sunrise"])),
    #         sunset=datetime.datetime.fromtimestamp(int(c["sunset"])),
    #         temperature=c.get("temperature"),
    #         wind_degree=c.get("wind_degree"),
    #         wind_speed=c.get("wind_speed")
    #
    #     )
        # fetch prediction
        # fetch last week weather
        # online learning
        # city, condition, predictid percent, rain_3h, snow_3h


#
if __name__ == '__main__':
    streaming = Streaming()
    streaming.run()
