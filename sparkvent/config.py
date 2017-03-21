# config parser
import yaml


class Config(object):
    def __init__(self, conf_path):
        try:
            with open(conf_path, 'r') as cfg_fd:
                conf = yaml.load(cfg_fd)

                if conf['mode'] == 'active':
                    self.server = conf['spark-master']
                elif conf['mode'] == 'history':
                    self.server = conf['history-server']
                self.period = conf['period']
                self.redis = conf['redis']
                self.type = conf['type']
        except IOError as e:
            print(e)
