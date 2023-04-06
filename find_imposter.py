import logging
from constant import *
from datetime import datetime, timezone
import pandas as pd
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)

CSV_FILE_NAME = 'SL Mapping Check (SL for CPIO & SL in Book9 - some missing)'
SAVE_LOG_FILE = "logs/" + CSV_FILE_NAME + CSV_FILE_TYPE
COL1 = 'to hide - Store Number'
COL2 = 'SL'
COL3 = 'is found'

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
    data_col2: list = df[COL2].values.tolist()
    result_list: list = list()
    for item in data_col2:
        if item not in data_col1:
            result_list.append(NOT_FILLED)
            logging.info("SL number: {0} not found in the column: {1}".format(item, COL1))
        else:
            result_list.append(item)
            logging.info("SL number: {0} found in the column: {1}".format(item, COL1))
    pd.DataFrame({
        COL1: data_col1,
        COL2: data_col2,
        COL3: result_list
        }).to_csv(
        BASE_FOLDER+CSV_FILE_NAME+RESULT+CSV_FILE_TYPE,
        encoding=ENCODING_TYPE_UTF8,
        index=False
    )
if __name__ == '__main__':
    main()
