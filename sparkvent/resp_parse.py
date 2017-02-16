# HTTP response (JSON) parser
import json


class AbstractParser(object):

    def __init__(self):
        pass

    def parse_json(self, job_json, parse_type):
        pass

    @staticmethod
    def get_parser(parser_type):
        if parser_type == "AppParser": return AppParser()
        if parser_type == "JobParser": return JobParser()


class AppParser(AbstractParser):

    def __init__(self):
        super(AppParser, self).__init__()
        self.apps = []

    def parse_json(self, app_json, parse_type):
        if parse_type == 'appid':
            return self.parse_app_id(app_json)

    def parse_app_id(self, app_json):
        jsd = json.loads(app_json)
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
        self.apps = apps
        return apps


class JobParser(AbstractParser):
    def __init__(self):
        super(JobParser, self).__init__()

    def parse_json(self, job_json, parse_type):
        pass


class StageParser(AbstractParser):
    def __init__(self):
        super(StageParser, self).__init__()

    def parse_json(self, stage_json, parse_type):
        pass

    def get_url(self, app_type):
        pass
