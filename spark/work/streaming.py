
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
import datetime
import json

CASSANDRA_FORMAT = "org.apache.spark.sql.cassandra"
TABLE_NAME = "raw_data"
KEY_SPACE = "weather_forecast"
TOPIC_NAME = "test"
BOOTSTRAP_SERVER= "172.17.0.1:9092"


def get_spark_session_instance(spark_conf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession \
            .builder \
            .config(conf=spark_conf) \
            .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]



def save_to_cassandra(rdd):
    if rdd.count() > 0:
        # NOTE: this should be alphabetical order
        schema = StructType([
            StructField("city", StringType(), True),
            StructField("clouds_all", IntegerType()),
            StructField("condition", StringType(), True),
            StructField("condition_details", StringType(), True),
            StructField("humidity", IntegerType(), True),
            StructField("latitude", FloatType(), True),
            StructField("longitude", FloatType(), True),
            StructField("measured_at", TimestampType(), True),
            StructField("pressure", IntegerType(), True),
            StructField("rain_3h", IntegerType()),
            StructField("snow_3h", IntegerType()),
            StructField("sunrise", TimestampType(), True),
            StructField("sunset", TimestampType(), True),
            StructField("temperature", FloatType(), True),
            StructField("wind_degree", IntegerType()),
            StructField("wind_speed", FloatType())
        ])


        spark = get_spark_session_instance(rdd.context.getConf())

        named_rdd = rdd.map(convert_to_row)
        stream_df = spark.createDataFrame(named_rdd, schema)
        stream_df.show()
        stream_df.write \
            .format(CASSANDRA_FORMAT) \
            .mode('append') \
            .options(table=TABLE_NAME, keyspace=KEY_SPACE) \
            .save()

def convert_to_row(c):
    # NOTE: this should be alphabetical order
    return Row(
        city=c.get("city"),
        condition=c.get("condition"),
        clouds_all=c.get("clouds_all"),
        condition_details=c.get("condition_details"),
        humidity=c.get("humidity"),
        latitude=c.get("latitude"),
        longitude=c.get("longitude"),
        measured_at=datetime.datetime.fromtimestamp(int(c["measured_at"])),
        pressure=c.get("pressure"),
        rain_3h=c.get("rain_3h"),
        snow_3h=c.get("snow_3h"),
        sunrise=datetime.datetime.fromtimestamp(int(c["sunrise"])),
        sunset=datetime.datetime.fromtimestamp(int(c["sunset"])),
        temperature=c.get("temperature"),
        wind_degree=c.get("wind_degree"),
        wind_speed=c.get("wind_speed")

    )


def main():
    conf = SparkConf(True).setMaster("local[*]").setAppName("jupyter pyspark").set("spark.cassandra.connection.host", "cassandra")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")

    sqlContext = SQLContext(sc)

    ssc = StreamingContext(sc, 2)

    weather_stream = KafkaUtils.createDirectStream(ssc, [TOPIC_NAME], {"metadata.broker.list": BOOTSTRAP_SERVER})
    parsed =  weather_stream.map(lambda v: json.loads(v[1]))

    #lines = weather_stream.map(lambda x: (x["city"], x["longitude"], x["latitude"]))
    parsed.pprint()
    logging.info(parsed)

    parsed.foreachRDD(lambda rdd: save_to_cassandra(rdd))


# not sure how to save to cassandra

    #lines.pprint()
    #raw_data = sqlContext.read.format(CASSANDRA_FORMAT).options(table=TABLE_NAME, keyspace= KEY_SPACE).load()
    #weather_stream_data.write.format(CASSANDRA_FORMAT).options(table=TABLE_NAME, keyspace = KEY_SPACE).save(mode ="append")
    ssc.start()
    ssc.awaitTermination()

if __name__ == '__main__':
    main()






