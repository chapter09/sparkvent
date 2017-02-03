# HTTP response (JSON) parser


class AbstractParser(object):
    def __init__(self):
        pass

    def get_url(self):
        pass


class AppParser(AbstractParser):
    def __init__(self):
        pass

    def get_url(self):
        pass


class JobParser(AbstractParser):
    def __init__(self):
        pass

    def get_url(self):
        pass


class StageParser(AbstractParser):
    def __init__(self):
        pass

    def get_url(self):
        pass



class ParserFactory(object):
    @staticmethod
    def get_parser():
        pass