from sparkvent.client import *
import pandas as pd


def main():

    client = Client("../conf/config.yml")

    parser = ParserFactory.get_parser('app', client.config.server)
    data = parser.get_data()
    data = pd.DataFrame.from_dict(data.values())

    # data_scale - exe  # -cpu#-mem#-table1-table2
    data['scale'] = data['name'].map(lambda name: name.split('-')[1])

    # remove rows with abnormal names
    data = data[data['name'].map(lambda name: name.startswith('Q23'))]

    data['exec_num'] = data['name'].map(lambda name: name.split('-')[2])
    data['cpu_num'] = data['name'].map(lambda name: name.split('-')[3])
    data['mem_num'] = data['name'].map(lambda name: name.split('-')[4])
    data['tbl1'] = data['name'].map(lambda name: name.split('-')[5])
    data['tbl2'] = data['name'].map(lambda name: name.split('-')[6])



    print data



if __name__ == '__main__':
    main()
