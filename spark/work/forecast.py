from pyspark.sql import SQLContext, SparkSession, Row
from pyspark.sql.types import *
TABLE_NAME = "prediction"
KEY_SPACE = "weather_forecast"
CASSANDRA_FORMAT = "org.apache.spark.sql.cassandra"
import logging

class Forecast(object):
    def fit(self, data):
        logging.critical(data)


    def predict(self):
        logging.critical("predict")


    def _save(self, rdd):
        if rdd.count() > 0:
            schema = self._get_schema()
            spark = self._get_spark_session_instance(rdd.context.getConf())

            named_rdd = rdd.map(self._convert_to_row)
            stream_df = spark.createDataFrame(named_rdd, schema)
            #stream_df.show()
            stream_df.write \
                .format(CASSANDRA_FORMAT) \
                .mode('append') \
                .options(table=TABLE_NAME, keyspace=KEY_SPACE) \
                .save()

    def _get_schema(self):
        return StructType([
            StructField("city", StringType(), True),
            StructField("condition", StringType(), True),
            StructField("forecast_on", DateType(), True),
            StructField("precipitation_percent", FloatType(), True),
            StructField("predicted_at", TimestampType(), True)
        ])


    def _get_spark_session_instance(self, spark_conf):
        if ("sparkSessionSingletonInstance" not in globals()):
            globals()["sparkSessionSingletonInstance"] = SparkSession \
                .builder \
                .config(conf=spark_conf) \
                .getOrCreate()
        return globals()["sparkSessionSingletonInstance"]

    def _convert_to_row(self, c):
        return Row(
            city=c.get("city"),
            condition=c.get("condition"),
            forecast_on=c.get("forecast_on"),
            precipitation_percent=c.get("precipitation_percent"),
            predicted_at=c.get("predicted_at")
        )


