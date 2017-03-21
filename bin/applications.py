from sparkvent.client import *


def main():

    client = Client("../conf/config.yml")

    #asdf = client.get_all_info()

    parser = ParserFactory.get_parser('stage', client.config.history_server)
    data = parser.get_data()
    print data
    client.store_info(data)


if __name__ == '__main__':
    main()
