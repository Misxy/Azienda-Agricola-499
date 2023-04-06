import logging
from constant import *
from datetime import datetime, timezone
import pandas as pd
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)

CSV_FILE_NAME = 'Raw Data for Rent Recievable (For Coding Sheet 1) 05JUN22'
SAVE_LOG_FILE = "logs/" + CSV_FILE_NAME + CSV_FILE_TYPE
COL1 = 'SL'
COL2 = 'OF'
COL3 = 'OF2'
COL4 = ' Rent Recievable '

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s | %(levelname)s | %(module)s | %(funcName)s | %(message)s',
                    handlers=[logging.FileHandler(SAVE_LOG_FILE+'-' +
                                                  datetime.now(timezone.utc).strftime("%d-%m-%Y-%H-%M-%S") +
                                                  LOG_EXTENSION, mode='w'),
                              stream_handler])


def main():
    df = pd.read_csv(BASE_FOLDER + CSV_FILE_NAME +
                     CSV_FILE_TYPE, encoding=ENCODING_TYPE_UTF8)
    print(df.columns.values.tolist())
    data_col1: list = df[COL1].values.tolist()
    data_col2: list = list(map(str,df[COL2].values.tolist()))
    data_col2 = list(map(lambda x: x[1:], data_col2))
    data_col3: list = list(map(str,df[COL3].values.tolist()))
    data_col3 = list(map(lambda x: x[:-2], data_col3))
    data_col4: list = df[COL4].values.tolist()

    result_list_col3: list = list()
    result_list_col4: list = list()
    for item in data_col2:
        try:
            target_index = data_col3.index(item)
            result_list_col3.append(data_col3[target_index])
            result_list_col4.append(data_col4[target_index])
            logging.info('Found the item: {0} at index: {1} with the value: {2}.'.format(
                item, target_index, data_col3[target_index]))
        except ValueError:
            result_list_col3.append(NOT_FILLED)
            result_list_col4.append(NOT_FILLED)
            logging.info('NOT found the item: {0} and uses the value: {1}.'.format(
                item, NOT_FILLED))
    pd.DataFrame({
        COL1: data_col1,
        COL2: data_col2,
        COL3: result_list_col3,
        COL4: result_list_col4
        }).to_csv(
        BASE_FOLDER+CSV_FILE_NAME+RESULT+CSV_FILE_TYPE,
        encoding=ENCODING_TYPE_UTF8,
        index=False
    )


if __name__ == '__main__':
    main()
