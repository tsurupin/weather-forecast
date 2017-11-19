#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
import functools
import logging
from crontab import CronTab
from datetime import datetime, timedelta
import math


class JobController(object):


    @classmethod
    def run(cls, crontab):

        def receive_func(job):
            @functools.wraps(job)
            def wrapper():

                job_settings = JobSettings(CronTab(crontab))
                logging.info("->- Process Start")
                while True:
                    try:
                        logging.info(
                            "-?- next running\tschedule:%s" %
                            job_settings.schedule().strftime("%Y-%m-%d %H:%M:%S")
                        )
                        time.sleep(job_settings.interval())
                        logging.info("->- Job Start")
                        job()
                        logging.info("-<- Job Done")
                    except KeyboardInterrupt:
                        break
                logging.info("-<- Process Done.")
            return wrapper
        return receive_func


class JobSettings(object):

    def __init__(self, crontab):

        self._crontab = crontab

    def schedule(self):
        crontab = self._crontab
        return datetime.now() + timedelta(seconds=math.ceil(crontab.next()))

    def interval(self):
        crontab = self._crontab
        return math.ceil(crontab.next())