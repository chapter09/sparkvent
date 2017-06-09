class Application(object):
    def __init__(self):
        self.id = ''
        self.name = ''
        self.duration = 0
        self.start_time = ''
        self.end_time = ''
        self.completed = False
        self.jobs = []


class Job(object):
    def __init__(self):
        self.id = None
        self.submission_time = ''
        self.completion_time = ''
        self.stage_ids = []
        self.status = ''
        self.num_tasks = 0
        self.num_active_tasks = 0
        self.num_completed_tasks = 0
        self.num_skipped_tasks = 0
        self.num_failed_tasks = 0
        self.num_active_stages = 0
        self.num_completed_stages = 0
        self.num_skipped_stages = 0
        self.num_failed_stages = 0


class Stage(object):
    def __init__(self):
        self.id = 0
        self.num_active_tasks = 0
        self.num_completed_tasks = 0
        self.num_failed_tasks = 0
        self.completion_time = ''
        self.input_size = 0
        self.output_size = 0
        

