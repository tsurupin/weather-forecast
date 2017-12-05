
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

CASSANDRA_FORMAT = "org.apache.spark.sql.cassandra"
TABLE_NAME = "raw_data"
KEY_SPACE = "weather_client"
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

        schema = StructType([
            StructField("city", StringType(), True),
            StructField("longitude", FloatType(), True),
            StructField("latitude", FloatType(), True),
            StructField("measured_at", TimestampType(), True)
        ])

        # rdd.foreach(lambda g: logging.info(g))
        # df = rdd.toDF()
        # df.show()


        # need to figure out how to convert datastream to dataframe
        #schema =
        spark = get_spark_session_instance(rdd.context.getConf())

        named_rdd = rdd.map(lambda c: Row(
            city=c[0],
            longitude=c[1],
            latitude=c[2],

            # condition=c["condition"],
            # condition_details=c["condition_details"],
            # temperature=c["temperature"],
            # pressure=c["pressure"],
            # humidity=c["humidity"],
            measured_at=datetime.datetime.fromtimestamp(int(c[3]))
        ))
        stream_df = spark.createDataFrame(named_rdd, schema)
        stream_df.show()
        stream_df.write \
            .format(CASSANDRA_FORMAT) \
            .mode('append') \
            .options(table=TABLE_NAME, keyspace=KEY_SPACE) \
            .save()


def main():
    conf = SparkConf(True).setMaster("local[*]").setAppName("jupyter pyspark").set("spark.cassandra.connection.host", "cassandra")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")

    sqlContext = SQLContext(sc)

    ssc = StreamingContext(sc, 2)

    weather_stream = KafkaUtils.createDirectStream(ssc, [TOPIC_NAME], {"metadata.broker.list": BOOTSTRAP_SERVER})
    lines = weather_stream.map(lambda x: (x["city"], x["longitude"], x["latitude"]))
    lines.pprint()
    logging.info(lines)

    lines.foreachRDD(lambda rdd: save_to_cassandra(rdd))


# not sure how to save to cassandra

    #lines.pprint()
    #raw_data = sqlContext.read.format(CASSANDRA_FORMAT).options(table=TABLE_NAME, keyspace= KEY_SPACE).load()
    #weather_stream_data.write.format(CASSANDRA_FORMAT).options(table=TABLE_NAME, keyspace = KEY_SPACE).save(mode ="append")
    ssc.start()
    ssc.awaitTermination()

if __name__ == '__main__':
    main()






