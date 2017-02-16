# HTTP response (JSON) parser
import json
from application import *

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
        if parse_type == 'jobid':
            return self.parse_jobid(job_json)
        return None

    def parse_jobid(self, job_json):
        jsd = json.loads(job_json)
        jobs = []

        for job in jsd:
            new_job = Job()
            new_job.id                   = job['jobId']
            new_job.submission_time      = job['submissionTime']
            new_job.completion_time      = job['completionTime']
            new_job.stage_ids            = job['stageIds']
            new_job.status               = job['status'] == 'SUCCEEDED'
            new_job.num_tasks            = job['numTasks']
            new_job.num_active_tasks     = job['numActiveTasks']
            new_job.num_completed_tasks  = job['numCompletedTasks']
            new_job.num_skipped_tasks    = job['numSkippedTasks']
            new_job.num_failed_tasks     = job['numFailedTasks']
            new_job.num_active_stages    = job['numActiveStages']
            new_job.num_completed_stages = job['numCompletedStages']
            new_job.num_skipped_stages   = job['numSkippedStages']
            new_job.num_failed_stages    = job['numFailedStages']

            jobs.append(new_job)
        return jobs

class StageParser(AbstractParser):
    def __init__(self):
        super(StageParser, self).__init__()

    def parse_json(self, stage_json, parse_type):
        pass

    def get_url(self, app_type):
        pass
