# HTTP response (JSON) parser
import json
from application import *

class AbstractParser(object):

    def __init__(self):
        pass

    def parse_json(self, job_json, parse_type):
        pass

    def get_redis_entry(self, app_json):
        pass

    @staticmethod
    def get_parser(parser_type):
        if parser_type == "AppParser": return AppParser()
        if parser_type == "JobParser": return JobParser()
        if parser_type == "StageParser": return StageParser()


class AppParser(AbstractParser):

    def __init__(self):
        super(AppParser, self).__init__()
        self.apps = []

    def parse_json(self, app_json, parse_type):
        if parse_type == 'appid':
            return self.parse_app_id(app_json)

    def get_redis_entry(self, app_json):
        jsd = json.loads(app_json)
        entries = {}
        for app in jsd:
            entry_key = app['id']
            entry_value = {
                'duration': str(app['attempts'][0]['duration']),
                'start_time': app['attempts'][0]['startTime'],
                'end_time': app['attempts'][0]['startTime'],
            }
            entries[entry_key] = entry_value
        return entries

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
        if parse_type == 'stageid':
            return self.parse_stage_id(stage_json)
        elif parse_type == 'attemptid':
            return self.parse_attempt_id(attempt_json=stage_json)
        return None

    def parse_stage_id(self, stage_json):
        jsd = json.loads(stage_json)
        stages = []

        for stage in jsd:
            new_stage = Stage()
            new_stage.id = stage['stageId']
            new_stage.num_active_tasks = stage['numActiveTasks']
            new_stage.num_completed_tasks = stage['numCompleteTasks']
            new_stage.num_failed_tasks = stage['numFailedTasks']
            stages.append(new_stage)

        return stages

    def parse_attempt_id(self, attempt_json):
        pass

    def get_url(self, app_type):
        pass
