# HTTP response (JSON) parser
from url_gen import UrlGen

class AbstractParser(object):
    def __init__(self):
        pass

    def get_url(self):
        raise NotImplementedError


class AppParser(AbstractParser):
    GET_APPID = '/application'

    def __init__(self):
        super(AppParser, self).__init__()
        self.aID = []  # all application IDs

    def get_url(self):
        pass

    class Factory:
        def create(self): return AppParser()


class JobParser(AbstractParser):
    def __init__(self):
        super(JobParser, self).__init__()

    def get_url(self):
        pass

    class Factory:
        def create(self): return JobParser()


class StageParser(AbstractParser):
    def __init__(self):
        super(StageParser, self).__init__()

    def get_url(self):
        pass

    class Factory:
        def create(self): return StageParser()


class ParserFactory(object):
    types = {}

    @staticmethod
    def get_parser(id):
        if not ParserFactory.types.has_key(id):
            ParserFactory.types[id] = eval(id + '.Factory()')
        return ParserFactory.types[id].create()
