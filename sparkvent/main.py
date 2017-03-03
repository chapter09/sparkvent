from client import *
from os import path
from config import Config

import json


def main():

    client = Client("../conf/config.yml")

    maps = {
        "3": "c",
    }
    client.store_info(maps)

    client.get_all_info()

    a = client.get_all_jobs_from_application('app-20170206013352-0006')
    jsd = json.loads(a)

    apps = []

    for app in jsd:
        app_dict = {}
        # for every appid
        for key, value in app.iteritems():
            if key == 'id':
                app_dict['id'] = value
            if key == 'attempts':
                app_dict['duration'] = str(value[0]['duration'])
                app_dict['start_time'] = value[0]['startTime']
                app_dict['end_time'] = value[0]['endTime']
        apps.append(app_dict)

    print apps


if __name__ == '__main__':
    main()
