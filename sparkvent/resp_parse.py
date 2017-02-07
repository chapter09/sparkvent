# HTTP response (JSON) parser
from url_gen import *
from http_req import *


class AbstractParser(object):
    def __init__(self):
        pass

    def get_url(self, app_type):
        raise NotImplementedError

    @staticmethod
    def factory(type):
        if type == "AppParser": return AppParser()
        if type == "JobParser": return JobParser()


class AppParser(AbstractParser):
    GET_APPID = '/applications'

    def __init__(self):
        super(AppParser, self).__init__()
        self.aID = []  # all application IDs

    def get_basic_app_info(self):
        request_url = self.get_url("app")
        app_info = get_json_from_address(request_url)
        return app_info

    def get_url(self, app_type):
        if (app_type == "app"):
            return generate_url('142.150.208.177', self.GET_APPID, {})


class JobParser(AbstractParser):
    def __init__(self):
        super(JobParser, self).__init__()

    def get_url(self, app_type):
        pass


class StageParser(AbstractParser):
    def __init__(self):
        super(StageParser, self).__init__()

    def get_url(self, app_type):
        pass
