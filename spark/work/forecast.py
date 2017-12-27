from pyspark.sql import SQLContext, SparkSession, Row
from pyspark.sql.types import *
TABLE_NAME = "prediction"
KEY_SPACE = "weather_forecast"
CASSANDRA_FORMAT = "org.apache.spark.sql.cassandra"
import logging
import pickle
from sklearn.linear_model import SGDRegressor
from feature_transformer import FeatureTransformer

FORECAST_MODEL_PICKLE_PATH = 'forecast_model'

class Forecast(object):
    def __init__(self, type, original_data=None):
        self.type = type;
        self.original_data = original_data
        if type == "streaming":
            with open(FORECAST_MODEL_PICKLE_PATH, mode='rb') as f:
                self.model = pickle.load(f)
        else:
           self.model = SGDRegressor(random_state=42, eta0=0.01, alpha=0.001, penalty='l1')

    def preprocess(self):
        if self.type == "batch":
            self.original_data = _load_data_from_cassandra()
        feature_transformer = FeatureTransformer(data=self.original_data)
        self.x, self.y = feature_transformer.perform()

    def fit(self):
        if self.x is not None and self.y is not None:
            self.model.partial_fit(self.x, self.y)
            with open(FORECAST_MODEL_PICKLE_PATH, mode='wb') as f:
                pickle.dump(FORECAST_MODEL_PICKLE_PATH, f)
            logging.critical(data)

    def predict(self, targetX):
        self.model.predict(targetX)
        logging.critical("predict")

    def _load_data_from_cassandra(self):
        session = cluster.connect(KEYSPACE_NAME)
        session.row_factory = pandas_factory
        session.default_fetch_size = 10000000
        rows = session.execute(sql)
        weather_data = rows._current_rows
        target_column = 'target_temp'
        data_columns = list(set(weather_data)) - [target_column]
        return weather_data[data_columns], weather_data['target_temp']





        # normalize data
        # update pickle
        # update prection table with plus one versiobn


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
