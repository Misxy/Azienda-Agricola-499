import logging
from turtle import heading

from numpy import float64
from constant import *
from datetime import datetime, timezone
import pandas as pd
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)
import math

CSV_FILE_NAME = 'SL for Wholesale'
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
    headers: list = df.columns.values.tolist()
    data_col1: list = df[headers[0]].values.tolist()
    data_col2: list = df[headers[1]].values.tolist()
    data_col3: list = df[headers[2]].values.tolist()
    data_col4: list = list(map(str,df[headers[3]].values.tolist()))
    data_col4 = list(map(lambda x: x[:-2], data_col4))
    targetValue = df[headers[4]].iloc[0]
    adder_col2 = list()
    adder_col3 = list()
    for item in data_col1:
        try:
            targetIdx = data_col4.index(item)
            if data_col2[targetIdx] == NOT_FILLED or \
                data_col3[targetIdx] == NOT_FILLED :
                adder_col2.append(targetValue)
                adder_col3.append(targetValue)
                continue
            adder_col2.append(
                float(data_col2[targetIdx]) + targetValue)
            adder_col3.append(
                float(data_col3[targetIdx]) + targetValue)
            logging.info('Store number: {0} is matched and operated.'.format(
                item
            ))
        except:
            adder_col2.append(data_col2[data_col1.index(item)])
            adder_col3.append(data_col3[data_col1.index(item)])
            logging.info('The item: {0} does not match with others'.format(item))
    pd.DataFrame({
        headers[0]: data_col1,
        headers[1]: adder_col2,
        headers[2]: adder_col3
        }).to_csv(
        BASE_FOLDER+CSV_FILE_NAME+RESULT+CSV_FILE_TYPE,
        encoding=ENCODING_TYPE_UTF8,
        index=False
    )
if __name__ == '__main__':
    main()
