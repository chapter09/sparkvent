from client import *
from os import path
from config import Config

import json


def main():

    client = Client("../conf/config.yml")

    all_apps = client.get_all_applications({'type': 'redis'})
    client.store_info(all_apps)


if __name__ == '__main__':
    main()
