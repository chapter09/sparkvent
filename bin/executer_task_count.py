#!/usr/bin/python
import sys
sys.path.insert(0, '../sparkvent')

import os
import time
import datetime
import redis

from sparkvent.client import *
from sparkvent.config import Config



ROOT_DIR = os.path.dirname(os.path.abspath(__file__ + "/../"))

def main():

    client = Client(ROOT_DIR + "/conf/config.yml.template")

    config = Config(os.path.abspath(ROOT_DIR + "/conf/config.yml.template"))
    redis_host, redis_port = config.redis.split(":")
    db = redis.Redis(host=redis_host, port=redis_port)

    timestamp = datetime.datetime.now()
    base_key = "executor:" + str(timestamp)

    while True:
        # get all info about all the executors for all applications
        executors = client.get_executor_info()
        if executors:
            store = {}
            for executor in executors:
                store[executor['executor'][0]['hostPort']] = executor['executor'][0]
            db.hset(base_key, timestamp, store)
            print store
            time.sleep(config.period)

if __name__ == '__main__':
    main()
