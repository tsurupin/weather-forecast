#!/usr/bin/env python
# -*- coding: utf-8 -*-
# https://qiita.com/shinyorke/items/77cd54bc836227b2416a
import logging
from multiprocessing import Pool
from scheduler.job import JobController
from weather_client.client import WeatherClient
logging.basicConfig(level=logging.DEBUG)
TOPIC_NAME = "test"
BOOTSTRAP_SERVER = '172.17.0.1'
#
weather_client = None
@JobController.run("*/1 * * * *")
def fetch_weather_data():
    try:
        global weather_client
        if weather_client is None:
            weather_client = WeatherClient()
        logging.critical("----------start fetching-------")


        weather_client.run()
    except Exception as e:
        logging.error(e)

def main():

    logging.basicConfig(
        level=logging.INFO,
        format="time:%(asctime)s.%(msecs)03d\tprocess:%(process)d" + "\tmessage:%(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )


    jobs = [fetch_weather_data]

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