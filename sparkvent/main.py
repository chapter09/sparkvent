from resp_parse import *
from os import path
from config import Config


def main():
    config = Config(path.abspath("../conf/config.yml")).conf

    app = AbstractParser.factory("AppParser")
    job = ParserFactory.get_parser("Job")

    a = app.get_basic_app_info()
    b = job.get_all_jobs_from_app('app-20170206013352-0006')

    print a


if __name__ == '__main__':
    main()
