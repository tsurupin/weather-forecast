
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '\
--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0,com.datastax.spark:spark-cassandra-connector_2.11:2.0.1 \
pyspark-shell'

from pyspark import SparkConf, SparkContext
#    Spark Streaming
from pyspark.streaming import StreamingContext
#    Kafka
from pyspark.streaming.kafka import KafkaUtils

from pyspark.sql import SQLContext

def main():
    conf = SparkConf(True).setMaster("local[*]").setAppName("jupyter pyspark").set("spark.cassandra.connection.host", "cassandra")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")

    ssc = StreamingContext(sc, 2)
    topic = "test"
    brokers = "172.17.0.1:9092"
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    lines = kvs.map(lambda x: x[1])
    lines.pprint()
    ssc.start()
    ssc.awaitTermination()

if __name__ == '__main__':
    main()






