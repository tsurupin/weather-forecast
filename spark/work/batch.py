

import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '\
--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0,com.datastax.spark:spark-cassandra-connector_2.11:2.0.1 \
pyspark-shell'

from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from pyspark.sql import SQLContext, SparkSession, Row
from pyspark.sql.types import *
import logging
import json
import WeatherForecast

CASSANDRA_FORMAT = "org.apache.spark.sql.cassandra"
TABLE_NAME = "raw_data"
KEY_SPACE = "weather_forecast"
TOPIC_NAME = "batch_processing"
BOOTSTRAP_SERVER= "172.17.0.1:9092"

def predict_weather(sc):
    sqlContext = SQLContext(sc)
    raw_data = sqlContext.read.format(CASSANDRA_FORMAT).options(table=TABLE_NAME, keyspace= KEY_SPACE).load()
    update_forecast(raw_data)

def update_forecast(data):
    forecast = WeatherForecast()
    forecast.fit(data)
    forecast.predict()

def main():
    conf = SparkConf(True).setMaster("local[*]").setAppName("jupyter pyspark").set("spark.cassandra.connection.host", "cassandra")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")

    ssc = StreamingContext(sc, 2)


    weather_stream = KafkaUtils.createDirectStream(ssc, [TOPIC_NAME], {"metadata.broker.list": BOOTSTRAP_SERVER})
    parsed =  weather_stream.map(lambda v: json.loads(v[1]))
    if parsed.count() > 1:
        predict_weather(sc)

    ssc.start()
    ssc.awaitTermination()

if __name__ == '__main__':
    main()
