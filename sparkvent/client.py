from http_req import *
from url_gen import *
from resp_parse import *


# need to make a thread to query periodically
class Client(object):

    def __init__(self, config):
        self.config = config
        self.url_gen = UrlGen()
        self.requester = HttpRequester()
        self.app_parser = AppParser()
        self.job_parser = JobParser()
        self.stage_parser = StageParser()

    # get all available info (apps, jobs, stages)
    def get_all_info(self):
        pass

    def get_all_applications(self):
        rest_api = ''
        data = self._get_data(rest_api, self.app_parser)
        return data

    def get_all_jobs_from_application(self, app_id):
        rest_api = app_id + '/' + 'jobs'
        data = self._get_data(rest_api, self.job_parser)
        return data

    def get_job_from_application(self, app_id, job_id):
        rest_api = app_id + '/' + 'jobs' + '/' + job_id
        data = self._get_data(rest_api, self.job_parser)
        return data

    def _get_data(self, rest_api, parser):
        url = self.url_gen.get_url(self.config.history_server, rest_api)
        self.requester.add_url(url)
        json_response = self.requester.make_request()

        #response = parser.parse_json(json_response)
        #return response
        return json_response
