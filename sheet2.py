import logging
from constant import *
from datetime import datetime, timezone
import pandas as pd
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)

CSV_FILE_NAME = 'Raw Data for Rent Payable (Sheet 2)'
SAVE_LOG_FILE = "logs/" + CSV_FILE_NAME + CSV_FILE_TYPE
COL1 = 'SL'
COL2 = 'OF'
COL3 = 'OF2'
COL4 = 'Rent'

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s | %(levelname)s | %(module)s | %(funcName)s | %(message)s',
                    handlers=[logging.FileHandler(SAVE_LOG_FILE+'-' +
                                                  datetime.now(timezone.utc).strftime("%d-%m-%Y-%H-%M-%S") +
                                                  LOG_EXTENSION, mode='w'),
                              stream_handler])


def main():
    df = pd.read_csv(BASE_FOLDER + CSV_FILE_NAME +
                     CSV_FILE_TYPE, encoding=ENCODING_TYPE_UTF8)
    print(df.head())
    data_col1: list = df[COL1].values.tolist()
    data_col2: list = list(map(str,df[COL2].values.tolist()))
    data_col3: list = list(map(str,df[COL3].values.tolist()))
    data_col3 = list(map(lambda x: x[:-2], data_col3))
    data_col4: list = df[COL4].values.tolist()
    result_list: list = list()
    for item in data_col2:
        try:
            result_list.append(data_col4[data_col3.index(item)])
            logging.info('The item: {0} found and marked with: {1}'.format(
                item,  data_col4[data_col3.index(item)]))
        except ValueError:
            result_list.append(NOT_FILLED)
            logging.info('The item: {0} was not found and marked with: {1}'.format(
                item,  NOT_FILLED)) 
    #Cost center main point
    pd.DataFrame({
        COL1: data_col1,
        COL3: result_list}).to_csv(
        BASE_FOLDER+CSV_FILE_NAME+RESULT+CSV_FILE_TYPE,
        encoding=ENCODING_TYPE_UTF8,
        index=False
    )


if __name__ == '__main__':
    main()
