#!/usr/bin/python
import sys
sys.path.insert(0, '../sparkvent')

from sparkvent.config import Config
from sparkvent.resp_parse import *
import os
import time
import datetime
import redis


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
            store = {}
            timestamp = datetime.datetime.now()
            for stage, value in data.items():
                store[stage] = {
                    'numActiveTasks': value['numActiveTasks'],
                    'numCompleteTasks': value['numCompleteTasks'],
                    'numFailedTasks': value['numFailedTasks'],
                }
            db.hset(base_key, timestamp, data)
            time.sleep(config.period)


if __name__ == '__main__':
    main()
