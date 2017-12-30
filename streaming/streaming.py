
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
import logging
import datetime
import json
from cassandra.cluster import Cluster
from cassandra.query import ordered_dict_factory, dict_factory
from kafka import KafkaConsumer
#from forecast import Forecast
from time import sleep
import logging
logging.basicConfig(level=logging.DEBUG)

KEYSPACE_NAME = "weather_forecast"
TABLE_NAME = "raw_data"
TOPIC_NAME = "streaming_data"
BOOTSTRAP_SERVER= "172.17.0.1"

class Streaming(object):

    def run(self):
        sleep(15)

        consumer = KafkaConsumer(TOPIC_NAME, bootstrap_servers=[BOOTSTRAP_SERVER])

        logging.info("streaming gets consumer----------")
        while True:
            need_prediction = False
            for msg in consumer:
                logging.info('streaming_data: {}'.format(msg))
                if msg.value is not None:
                    need_prediction = True
            # if need_prediction:
            #     #self._save(msg.value)
            #     #self._predict_weather()
            logging.info("load_data-------------")
            sleep(300)

        consumer.close()

    def _save(self, data):
        cluster = Cluster([os.environ.get('CASSANDRA_PORT_9042_TCP_ADDR', 'localhost')],
                          port=int(os.environ.get('CASSANDRA_PORT_9042_TCP_PORT', 9042))
                          )
        session = cluster.connect(KEYSPACE_NAME)
        self.session.row_factory = dict_factory
        # need to think about uuid
        self.session.execute("INSERT INTO raw_data (id, location) VALUES (%s, %s)",
                (uuid(), data['dt'], data['dt_iso'], data['city_id'], data['city_name'], data['temperature'], data['temperature_max'], data['temperature_min'], data['pressure'], data['humidity'], data['wind_degree'],data['wind_speed'], data['rain_3h'], data['snow_3h'], data['clouds_all'], data['condition_id'], data['condition_details'], Time.now() ))

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
