import os
import sys


# from pyspark.sql import SQLContext, SparkSession, Row
# from pyspark.sql.types import *
# CASSANDRA_FORMAT = "org.apache.spark.sql.cassandra"
import logging
import pickle
import pandas as pd
sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/../'))

from config import *
from time import sleep
from datetime import datetime
from cassandra.cluster import Cluster
from cassandra.query import ordered_dict_factory
from sklearn.linear_model import SGDRegressor
from feature_transformer import FeatureTransformer
SAN_FRANCISCO_CITY_NAME = 'san francisco'
LOADING_DATA_NUM_FOR_STREAMING = 50

class Forecast(object):
    def __init__(self, type, original_data=None):
        self.type = type;
        self.original_data = original_data
        self.x = None
        self.y = None
        self.target_x = None
        self.session = None
        self.prediction_result = None

        if type == "streaming" and os.path.exists(TEMPERATURE_FORECAST_MODEL_PICKLE_PATH) and os.path.getsize(TEMPERATURE_FORECAST_MODEL_PICKLE_PATH) > 0:
            with open(TEMPERATURE_FORECAST_MODEL_PICKLE_PATH, mode='rb') as f:
                self.model = pickle.load(f)
        else:
           self.model = SGDRegressor(random_state=42, eta0=0.01, alpha=0.001, penalty='l1')


    def preprocess(self):
        original_data = self._load_data_from_cassandra()

        feature_transformer = FeatureTransformer(data=original_data)
        self.x, self.y, self.target_x, self.forecast_at = feature_transformer.perform()
        logging.critical('x={}, y={}, target_x={}'.format(self.x, self.y, self.target_x))

    def fit(self):
        if self.x is not None and self.y is not None:
            self.model.partial_fit(self.x, self.y)
            with open(TEMPERATURE_FORECAST_MODEL_PICKLE_PATH, mode='wb') as f:
                pickle.dump(self.model, f)

    def predict(self):
        if self.target_x is not None:
            self.prediction_result = self.model.predict(self.target_x)[0]
            logging.critical('predict: {}'.format(self.prediction_result))

    def save(self):
        if self.prediction_result is not None:
            latest_version = self._latest_version()
            self.session.execute('''
            INSERT INTO {} (temperature, city_name, version, predicted_at, forecast_at)
            VALUES (%s, %s, %s, %s, %s)
            '''.format(PREDICTION_TABLE_NAME), (self.prediction_result, SAN_FRANCISCO_CITY_NAME, latest_version, datetime.now(), datetime.utcfromtimestamp(self.forecast_at)))

    def _load_data_from_cassandra(self):
        self.session = self._load_cassandra_session()
        self.session.row_factory = self._pandas_factory
        self.session.default_fetch_size = None #10000000
        sql = "SELECT * FROM {0} where city_name = '{1}'".format(RAW_DATA_TABLE_NAME, SAN_FRANCISCO_CITY_NAME)
        sql += ' LIMIT {}'.format(LOADING_DATA_NUM_FOR_STREAMING) if self.type == 'streaming' else ''
        rows = self.session.execute(sql)
        weather_data = rows._current_rows
        return weather_data

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

    def _pandas_factory(self,colnames, rows):
        return pd.DataFrame(rows, columns=colnames)

    def _latest_version(self):
        rows = self.session.execute('SELECT DISTINCT version, city_name FROM {}'.format(PREDICTION_TABLE_NAME))
        version_list = list(rows._current_rows['version'])
        latest_version = max(version_list) if any(version_list) else 1
        if self.type == 'batch':
            latest_version += 1
        return latest_version

    # def _save(self, rdd):
    #     if rdd.count() > 0:
    #         schema = self._get_schema()
    #         spark = self._get_spark_session_instance(rdd.context.getConf())
    #
    #         named_rdd = rdd.map(self._convert_to_row)
    #         stream_df = spark.createDataFrame(named_rdd, schema)
    #         #stream_df.show()
    #         stream_df.write \
    #             .format(CASSANDRA_FORMAT) \
    #             .mode('append') \
    #             .options(table=PREDICTION_TABLE_NAME, keyspace=KEY_SPACE) \
    #             .save()
    #
    # def _get_schema(self):
    #     return StructType([
    #         StructField("city", StringType(), True),
    #         StructField("condition", StringType(), True),
    #         StructField("forecast_on", DateType(), True),
    #         StructField("precipitation_percent", FloatType(), True),
    #         StructField("predicted_at", TimestampType(), True)
    #     ])
    #
    #
    # def _get_spark_session_instance(self, spark_conf):
    #     if ("sparkSessionSingletonInstance" not in globals()):
    #         globals()["sparkSessionSingletonInstance"] = SparkSession \
    #             .builder \
    #             .config(conf=spark_conf) \
    #             .getOrCreate()
    #     return globals()["sparkSessionSingletonInstance"]
    #
    # def _convert_to_row(self, c):
    #     return Row(
    #         city=c.get("city"),
    #         condition=c.get("condition"),
    #         forecast_on=c.get("forecast_on"),
    #         precipitation_percent=c.get("precipitation_percent"),
    #         predicted_at=c.get("predicted_at")
    #     )
