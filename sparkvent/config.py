# config parser

import yaml


class Config(object):
    def __init__(self, conf_path):
        try:
            with open(conf_path, 'r') as cfg_fd:
                self.conf = yaml.load(cfg_fd)
        except FileNotFoundError as e:
            print(e)

    def get(self, key):
        pass
