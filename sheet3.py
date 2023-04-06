import logging
from operator import truth
from xml.dom import NotFoundErr
from constant import *
from datetime import datetime, timezone
import pandas as pd
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)

CSV_FILE_NAME = 'Raw Data for Rent Recievable (For Coding)'
SAVE_LOG_FILE = "logs/" + CSV_FILE_NAME + CSV_FILE_TYPE
COL1 = 'to hide - Store Number'
COL2 = 'SL'
COL3 = 'OF'
COL4 = 'OF2'
COL5 = 'Rent Recievable'

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s | %(levelname)s | %(module)s | %(funcName)s | %(message)s',
                    handlers=[logging.FileHandler(SAVE_LOG_FILE+'-' +
                                                  datetime.now(timezone.utc).strftime("%d-%m-%Y-%H-%M-%S") +
                                                  LOG_EXTENSION, mode='w'),
                              stream_handler])


def main():
    df = pd.read_csv(BASE_FOLDER + CSV_FILE_NAME +
                     CSV_FILE_TYPE, encoding=ENCODING_TYPE_UTF8)
    data_col1: list = df[COL1].values.tolist()
    data_col2: list = df[COL2].values.tolist()
    data_col3: list = df[COL3].values.tolist()
    data_col4: list = list(map(str, df[COL4].values.tolist()))
    data_col4 = list(map(lambda x: x[:-2], data_col4))
    data_col5: list = df[COL5].values.tolist()
    col1_matched: list = list()
    col2_matched: list = list()
    col3_matched: list = list()
    for item in data_col4:
        try:
            col3_matched.append(data_col3[data_col3.index(item)])
            logging.info('The item: {0} was found and marked with: {1}'.format(
                item,  data_col3[data_col3.index(item)]))
        except ValueError:
            col3_matched.append(item)
            logging.info('The item: {0} was not found and marked with: {1}'.format(
                item,  item)) 
    
    for item in col3_matched:
        try:
            target_idx = data_col2.index(item)
            col1_matched.append(data_col1[target_idx])
            col2_matched.append(data_col2[target_idx])
            logging.info('Item: {0} was found.\nColumn: {1} marked with the value:{2}\nColumn: {3} marked with the value: {4}'.format(
               item,COL1, data_col1[target_idx], COL2, data_col2[target_idx]))
        except ValueError: 
            col1_matched.append(data_col1[col3_matched.index(item)])
            col2_matched.append(NOT_FILLED)
            logging.info('Item: {0} was not found.\nColumn: {1} marked with the value:{2}\nColumn: {3} marked with the value: {4}'.format(
               item,COL1, NOT_FILLED, COL2, item))
    pd.DataFrame({
        COL1: col1_matched,
        COL2: col2_matched,
        COL3: col3_matched,
        COL4: data_col4,
        COL5: data_col5}).to_csv(
        BASE_FOLDER+CSV_FILE_NAME+RESULT+CSV_FILE_TYPE,
        encoding=ENCODING_TYPE_UTF8,
        index=False
    )


if __name__ == '__main__':
    main()
