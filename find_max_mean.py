import logging
from constant import *
from datetime import datetime, timezone
import pandas as pd
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)


CSV_FILE_NAME = 'Building Improvement for SUM UP Per Store'
SAVE_LOG_FILE = "logs/" + CSV_FILE_NAME + CSV_FILE_TYPE

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s | %(levelname)s | %(module)s | %(funcName)s | %(message)s',
                    handlers=[logging.FileHandler(SAVE_LOG_FILE+'-' +
                                                  datetime.now(timezone.utc).strftime("%d-%m-%Y-%H-%M-%S") +
                                                  LOG_EXTENSION, mode='w'),
                              stream_handler])


def main():
    df = pd.read_csv(BASE_FOLDER + CSV_FILE_NAME +
                     CSV_FILE_TYPE, encoding=ENCODING_TYPE_UTF8)
    headings: list = df.columns.values.tolist()
    COL1 = 'Cost Center/Store Number (ERP )'
    COL2 = 'Cost'

    data_col1: list = df[COL1].values.tolist()
    data_col2: list = df[COL2].values.tolist()
    list_dict: list = list()
    marked_list: list = list()
    for centre in data_col1:
        if centre in marked_list:
            continue
        all_indexes = find_all_indexes(data_col1, centre)
        # all_values = sorted([data_col1[idx]
        #                     for idx in all_indexes], reverse=True)
        # maximum = all_values[0]
        # average = sum(all_values) / len(all_values)
        # logging.info('Maximum is: {0} | Average is: {1}'.format(
        #     maximum, average))
        # list_dict.append(
        #     {
        #         COL2: centre,
        #         MAXIMUM: maximum,
        #         AVERAGE: average
        #     })
        summation = 0
        for idx in all_indexes:
            summation += data_col2[idx]
        list_dict.append(
            {
                COL1: centre,
                COL2: summation,
            }
        )
        marked_list.append(centre)
       
    df = pd.DataFrame.from_dict(list_dict)
    df.to_csv(
        BASE_FOLDER+CSV_FILE_NAME+RESULT+CSV_FILE_TYPE,
        index=False,
        header=True)


def find_all_indexes(ls: list, data: str):
    return [i for i, v in enumerate(ls) if v == data]


def get_distinct_data(ls: list):
    return sorted(set(ls))


if __name__ == '__main__':
    main()
