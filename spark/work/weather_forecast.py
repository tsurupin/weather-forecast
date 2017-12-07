from pyspark.sql import SQLContext, SparkSession, Row
from pyspark.sql.types import *
 TABLE_NAME = "wprediction"
 KEY_SPACE = "weather_forecast"
 CASSANDRA_FORMAT = "org.apache.spark.sql.cassandra"

 class WeatherForecast(object):
    def __init__(self):

    def fit(self):
        return

    def predict(self):
        return

    def _save(self, rdd):
        if rdd.count() > 0:
            schema = StructType([
                StructField("city", StringType(), True),
                StructField("condition", StringType(), True),
                StructField("forecast_on", DateType(), True),
                StructField("predicted_at", TimestampType(), True),
                StructField("prediction_percent", FloatType(), True),
                StructField("rain_3h", IntegerType()),
                StructField("snow_3h", IntegerType())
            ])


            spark = _get_spark_session_instance(rdd.context.getConf())

            named_rdd = rdd.map(_convert_to_row)
            stream_df = spark.createDataFrame(named_rdd, schema)
            #stream_df.show()
            stream_df.write \
                .format(CASSANDRA_FORMAT) \
                .mode('append') \
                .options(table=TABLE_NAME, keyspace=KEY_SPACE) \
                .save()

    def _convert_to_row(c):
        return Row(
            city=c.get("city"),
            condition=c.get("condition"),
            forecast_on=c.get("forecast_on"),
            predicted_at=c.get("predicted_at"),
            prediction_percent=c.get("prediction_percent"),
            rain_3h=c.get("rain_3h"),
            snow_3h=c.get("snow_3h")
        )

    def _get_spark_session_instance(spark_conf):
        if ("sparkSessionSingletonInstance" not in globals()):
            globals()["sparkSessionSingletonInstance"] = SparkSession \
                .builder \
                .config(conf=spark_conf) \
                .getOrCreate()
        return globals()["sparkSessionSingletonInstance"]

