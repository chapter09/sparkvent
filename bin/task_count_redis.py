#!/usr/bin/python
import sys
import os
import time
import datetime
import redis

sys.path.insert(0, '../sparkvent')
from sparkvent.config import Config
from sparkvent.resp_parse import *

ROOT_DIR = os.path.dirname(os.path.abspath(__file__ + "/../"))


def main():
    config = Config(os.path.abspath(ROOT_DIR + "/conf/config.yml"))
    parser = ParserFactory.get_parser(config.type, config.server)
    redis_host, redis_port = config.redis.split(":")
    db = redis.Redis(host=redis_host, port=redis_port)

    timestamp = datetime.datetime.now()
    base_key = "task_count:" + str(timestamp)

    while True:
        data = parser.get_data()
        if data != {}:
            timestamp = datetime.datetime.now()
            db.hset(base_key, timestamp, data)
            time.sleep(config.period)


if __name__ == '__main__':
    main()
