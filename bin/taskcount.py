#!/usr/bin/python

import sys
import os
sys.path.insert(0, '../sparkvent')

from sparkvent.client import *
from sparkvent.config import Config
import csv

ROOT_DIR = os.path.realpath(__file__)

def main():
    client = Client("../conf/config.yml")
    all_data = client.get_all_info()
    task_count = get_task_count_for_all_apps(all_data)
    output_csv(task_count)


def get_task_count_for_app(stages):
    num_active = 0
    num_finished = 0
    num_failed = 0

    for stage in stages:
        num_active += stage.num_active_tasks
        num_finished += stage.num_completed_tasks
        num_failed += stage.num_failed_tasks

    return (num_active, num_finished, num_failed)


def get_task_count_for_all_apps(entries):
    # entries should be the dictionary containing entry: {'application': None, 'jobs': None, 'stages': None}
    result = {}
    for entry in entries:
        app = entry['application']
        task_count = get_task_count_for_app(entry['stages'])
        result[app['id']] = task_count

    return result

def output_csv(task_count):
    # task_count is the data output by get_task_count_for_all_apps
    if not os.path.exists("../output/"):
        os.mkdir("../output/")

    with open('../output/mycsvfile.csv', 'wb') as f:  # Just use 'w' mode in 3.x
        writer = csv.writer(f)
        writer.writerow(["ID", "Active", "Finished", "Failed"])
        for key, value in task_count.items():
            writer.writerow([key, value[0], value[1], value[2]])


if __name__ == '__main__':
    main()
