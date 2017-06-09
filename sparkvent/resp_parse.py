# HTTP response (JSON) parser
import json
from application import *
from http_req import *
from url_gen import *


class AbstractParser(object):
    def __init__(self, server):
        self.server = server
        self.url_gen = UrlGen()
        self.requester = HttpRequester()

    def parse_json(self, json_input, parse_type):
        pass

    def get_data(self):
        pass
    
    def get_sample_data(self):
        pass

    def _get_response(self, rest_api):
        url = self.url_gen.get_url(self.server, rest_api)
        json_string = self.requester.single_request(url)
        return json_string


class AppParser(AbstractParser):

    def __init__(self, server):
        super(AppParser, self).__init__(server)
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
            print app
            exit(0)
            for key, value in app.iteritems():
                if key == 'id':
                    app_dict['id'] = value
                if key == 'name':
                    app_dict['name'] = value
                if key == 'attempts':
                    app_dict['duration'] = str(value[0]['duration'])
                    app_dict['start_time'] = value[0]['startTime']
                    app_dict['end_time'] = value[0]['endTime']
            apps.append(app_dict)
        self.apps = apps
        return apps

    def parse_json_redis(self, json_input):
        jsd = json.loads(json_input)
        entries = {}
        for app in jsd:
            entry_key = app['id']
            entry_value = {
                'id': app['id'],
                'name': app['name'],
                'duration': str(app['attempts'][0]['duration']),
                # 'start_time': app['attempts'][0]['startTime'],
                # 'end_time': app['attempts'][0]['startTime'],
            }
            entries[entry_key] = entry_value
        return entries
    
    def get_sample_data(self):
        rest_api = ''
        json_string = self._get_response(rest_api)
        response = dict(self.parse_json_redis(json_string).items()[:10])
        return response

    def get_data(self):
        rest_api = ''
        json_string = self._get_response(rest_api)
        response = self.parse_json_redis(json_string)
        return response


class JobParser(AbstractParser):
    def __init__(self, server):
        super(JobParser, self).__init__(server)
        self.app_parser = AppParser(server)

    def parse_json(self, job_json, parse_type):
        if parse_type == 'jobid':
            return self.parse_jobid(job_json)
        return None

    def parse_jobid(self, job_json):
        jsd = json.loads(job_json)
        jobs = []

        for job in jsd:
            new_job = Job()
            new_job.id = job['jobId']
            new_job.submission_time = job['submissionTime']
            new_job.completion_time = job['completionTime']
            new_job.stage_ids = job['stageIds']
            new_job.status = job['status'] == 'SUCCEEDED'
            new_job.num_tasks = job['numTasks']
            new_job.num_active_tasks = job['numActiveTasks']
            new_job.num_completed_tasks = job['numCompletedTasks']
            new_job.num_skipped_tasks = job['numSkippedTasks']
            new_job.num_failed_tasks = job['numFailedTasks']
            new_job.num_active_stages = job['numActiveStages']
            new_job.num_completed_stages = job['numCompletedStages']
            new_job.num_skipped_stages = job['numSkippedStages']
            new_job.num_failed_stages = job['numFailedStages']

            jobs.append(new_job)
        return jobs

    def parse_json_redis(self, json_input, app_id):
        entries = {}
        jsd = json.loads(json_input)
        for job in jsd:
            entry_key = app_id + ':jobs:' + str(job['jobId'])
            entry_value = {
                'jobId':                job['jobId'],
                'submissionTime':       job['submissionTime'],
                'completionTime':       job['completionTime'],
                'stageIds':             job['stageIds'],
                'status':               job['status'],
                'numTasks':             job['numTasks'],
                'numActiveTasks':       job['numActiveTasks'],
                'numCompletedTasks':    job['numCompletedTasks'],
                'numSkippedTasks':      job['numSkippedTasks'],
                'numFailedTasks':       job['numFailedTasks'],
                'numActiveStages':      job['numActiveStages'],
                'numCompletedStages':   job['numCompletedStages'],
                'numSkippedStages':     job['numSkippedStages'],
                'numFailedStages':      job['numFailedStages'],
            }
            entries[entry_key] = entry_value
        return entries

    def get_rest_api(self, app_id):
        rest_api = app_id + '/' + 'jobs'
        return rest_api

    def get_data(self):
        apps = self.app_parser.get_data()
        response = {}
        for app in apps.values():
            app_id = app['id']
            rest_api = self.get_rest_api(app_id)
            json_string = self._get_response(rest_api)
            response.update(self.parse_json_redis(json_string, app_id))
        return response


class StageParser(AbstractParser):
    def __init__(self, server):
        super(StageParser, self).__init__(server)
        self.app_parser = AppParser(server)

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
            new_stage.input_size = stage['shuffleReadBytes']
            new_stage.completion_time = stage['']
            stages.append(new_stage)

        return stages

    def parse_attempt_id(self, attempt_json):
        pass

    def parse_json_redis(self, json_input, app_id):
        entries = {}
        jsd = json.loads(json_input)
        for stage in jsd:
            entry_key = app_id + ':stages:' + str(stage['stageId'])
            entry_value = {
                'app_id': app_id,
                'stageId': stage['stageId'],
                'status': stage['status'],
                'attemptId': stage['attemptId'],
                'numActiveTasks': stage['numActiveTasks'],
                'numCompleteTasks': stage['numCompleteTasks'],
                'numFailedTasks': stage['numFailedTasks'],
            }
            entries[entry_key] = entry_value
        return entries

    def get_rest_api(self, app_id):
        rest_api = app_id + '/' + 'stages'
        return rest_api
    
    def get_sample_data(self):
        apps = dict(self.app_parser.get_data().items()[:10])
        response = {}
        for app in apps.values():
            app_id = app['id']
            rest_api = self.get_rest_api(app_id)
            json_string = self._get_response(rest_api)
            response.update(self.parse_json_redis(json_string, app_id))
        return response

    def get_data(self):
        apps = self.app_parser.get_data()
        response = {}
        for app in apps.values():
            app_id = app['id']
            rest_api = self.get_rest_api(app_id)
            json_string = self._get_response(rest_api)
            response.update(self.parse_json_redis(json_string, app_id))
        return response


class TaskParser(AbstractParser):
    def __init__(self, server):
        super(TaskParser, self).__init__(server)
        self.stage_parser = StageParser(server)

    def parse_json(self, json_input, parse_type):
        pass

    def parse_json_redis(self, json_input, app_id, stage_id):
        entries = {}
        jsd = json.loads(json_input)[0]['tasks']
        for task in jsd.values():
            entry_key = app_id + ':stages:' + stage_id + ':tasks:' + str(task['taskId'])
            entry_value = {
                'taskId': task['taskId'],
                'attempt': task['attempt'],
                'launchTime': task['launchTime'],
                'executorId': task['executorId'],
                'host': task['host'],
                'taskLocality': task['taskLocality'],
                'taskMetrics': task['taskMetrics'],
            }
            entries[entry_key] = entry_value
        return entries

    def get_data(self):
        stages = self.stage_parser.get_data()
        all_response = {}
        for key, stage in stages.iteritems():
            app_id = key.split(':')[0]
            stage_id = str(stage['stageId'])
            rest_api = self.get_rest_api(app_id, stage_id)
            json_string = self._get_response(rest_api)
            response = self.parse_json_redis(json_string, app_id, stage_id)
            all_response.update(response)
        return all_response

    def get_rest_api(self, app_id, stage_id):
        rest_api = app_id + '/' + 'stages' + '/' + stage_id
        return rest_api


class ExecParser(AbstractParser):
    def __init__(self, server):
        super(ExecParser, self).__init__(server)

    def parse_json(self, json_input, parse_type):
        # parse the executor json
        # not storing the info anymore
        jsd = json.loads(json_input)
        result = []

        for executor in jsd:
            if executor['id'] == 'driver':
                # not dealing with drivers here
                continue
            # not driver, drop many keys
            executor.pop('diskUsed', None)
            executor.pop('executorLogs', None)
            executor.pop('rddBlocks', None)
            executor.pop('maxMemory', None)
            executor.pop('totalTasks', None)
            executor.pop('maxTasks', None)
            executor.pop('memoryUsed', None)
            executor.pop('isActive', None)

            result.append(executor)

        return result


class ParserFactory(object):
    parser_types = {'app', 'job', 'stage', 'task', 'exec'}

    @staticmethod
    def get_parser(parser_type, server):
        if parser_type not in ParserFactory.parser_types:
            raise Exception("Unknown parser type!")

        if parser_type == "app":
            return AppParser(server)
        elif parser_type == "job":
            return JobParser(server)
        elif parser_type == "stage":
            return StageParser(server)
        elif parser_type == "exec":
            return ExecParser(server)
        else:
            return TaskParser(server)
