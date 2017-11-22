#!/usr/bin/env python
# -*- coding: utf-8 -*-
# https://qiita.com/shinyorke/items/77cd54bc836227b2416a
import logging
from multiprocessing import Pool
from scheduler.job import JobController
from weather_client.client import WeatherClient
from kafka import KafkaConsumer
logging.basicConfig(level=logging.DEBUG)
TOPIC_NAME = "test"
BOOTSTRAP_SERVER = '192.168.99.100'
#BOOTSTRAP_SERVER = "kafka"
#BOOTSTRAP_SERVER = "192.168.2.74"
@JobController.run("*/1 * * * *")
def fetch_weather_data():
    try:
        client = WeatherClient([1,2,3])
        client.run()
    except Exception as e:
        logging.error(e)

@JobController.run("*/1 * * * *")
def consume_weather_data():
    try:


        logging.critical("----------start consuming-------")

        consumer = KafkaConsumer(TOPIC_NAME, bootstrap_servers=[BOOTSTRAP_SERVER])
        logging.info(consumer)
        logging.critical("----------consumer-------")
        metrics = consumer.metrics()
        logging.info(metrics)


        logging.critical("----------end consuming-------")

        for msg in consumer:
            logging.info("--------------------------a")
            logging.info(msg)
        consumer.close()
    except Exception as e:
        logging.error(e)
def main():

    logging.basicConfig(
        level=logging.INFO,
        format="time:%(asctime)s.%(msecs)03d\tprocess:%(process)d" + "\tmessage:%(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )


    jobs = [fetch_weather_data, consume_weather_data]

    # multi process running
    p = Pool(len(jobs))
    logging.info(p)
    try:
        for job in jobs:
            p.apply_async(job)
        p.close()
        p.join()

    except KeyboardInterrupt:
        logging.info("exit")


if __name__ == '__main__':
    main()