import resp_parse


def main():

    app = resp_parse.AbstractParser.factory("AppParser")

    a = app.get_basic_app_info()

    print a


if __name__ == '__main__':
    main()