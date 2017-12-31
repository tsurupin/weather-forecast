import os
import sys
import uuid
import logging
import datetime
import json

from kafka import KafkaConsumer

from time import sleep
import logging
sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/shared'))
from config import *
sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/shared/predictions'))
from forecast import Forecast

class Batch(object):

    def run(self):
        sleep(30)

        consumer = self._load_kafka_consumer()
        self._consume_message(consumer)
        logging.critical("consumer gets close")
        consumer.close()

    def _consume_message(self, consumer):
        for msg in consumer:
            logging.critical("finaoyy got message-!!!!!!!!!!!!!!")
            logging.critical(msg)
            logging.critical(msg.value)

            logging.critical('batch_data: {}'.format(msg))

            self._predict_weather()
            logging.critical("load_data-------------")

    def _predict_weather(self):
        from forecast import Forecast
        forecast = Forecast(type="batch")
        forecast.preprocess()
        forecast.fit()
        prediction = forecast.predict()
        logging.critical('result: {}'.format(prediction))
        forecast.save()

    def _load_kafka_consumer(self):
        consumer = None
        while consumer is None:
            try:
                consumer = KafkaConsumer(
                    BATCH_DATA_TOPIC_NAME,
                    bootstrap_servers=[BOOTSTRAP_SERVER]
                )
                return consumer
            except Exception as e:
                logging.critical(e)
                sleep(5)


if __name__ == '__main__':
    batch = Batch()
    batch.run()
