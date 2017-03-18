# config parser
import yaml


class Config(object):
    def __init__(self, conf_path):
        try:
            with open(conf_path, 'r') as cfg_fd:
                conf = yaml.load(cfg_fd)
                self.spark_master = conf['spark-master']
                self.history_server = conf['history-server']
                self.period = conf['period']
                self.redis = conf['redis']
                self.type = conf['type']
        except FileNotFoundError as e:
            print(e)
