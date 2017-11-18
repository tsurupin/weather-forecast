#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
from multiprocessing import Pool
from scheduler.job import JobController

# Docker ImageのTimezoneがUTCなので注意！
@JobController.run("20 15 * * 5")
def notice_tmr_club():
    """
    タモリ倶楽部の時間だお(東京)
    :return: None
    """
    logging.info("タモリ倶楽部はじまるよ！！！")


# Docker ImageのTimezoneがUTCなので注意！(大切なので2回言いました)
@JobController.run("00 9 * * *")
def notice_baseball():
    """
    やきうの時間を教えるお
    :return: None
    """
    logging.info("やきうの時間だあああ！！！！")


def main():
    """
    crontabを動かすmethod
    :return: None
    """
    # ログ設定(Infoレベル、フォーマット、タイムスタンプ)
    logging.basicConfig(
        level=logging.INFO,
        format="time:%(asctime)s.%(msecs)03d\tprocess:%(process)d" + "\tmessage:%(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # crontabで実行したいジョブを登録
    jobs = [notice_tmr_club, notice_baseball]

    # multi process running
    p = Pool(len(jobs))
    try:
        for job in jobs:
            p.apply_async(job)
        p.close()
        p.join()
    except KeyboardInterrupt:
        logging.info("exit")


if __name__ == '__main__':
    main()