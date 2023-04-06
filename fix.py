import logging
from operator import truth
from xml.dom import NotFoundErr
from constant import *
from datetime import datetime, timezone
import pandas as pd
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)

CSV_FILE_NAME = 'Raw Data for Rent Recievable (For Coding)-final'
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

'''

Ran with:
Raw Data for Rent Recievable (For Coding)

'''
def main():
    df = pd.read_csv(BASE_FOLDER + CSV_FILE_NAME +
                     CSV_FILE_TYPE, encoding=ENCODING_TYPE_UTF8)
    data_col1: list = df[COL1].values.tolist()
    data_col2: list = list(map(str, df[COL2].values.tolist()))
    data_col3: list = list(map(str, df[COL3].values.tolist()))
    data_col4: list = list(map(str, df[COL4].values.tolist()))
    data_col5: list = df[COL5].values.tolist()

    col3_matched: list = list()
    col4_matched: list = list()
    col5_matched: list = list()
    
    final_col4_matched: list = list()
    final_col5_matched: list = list()
    for item in data_col3:
        try:
            target_index = data_col4.index(item)
            col4_matched.append(data_col4[target_index])
            col5_matched.append(data_col5[target_index])
            logging.info('Found the item: {0} at the column: {1} at the index: {2}'.format(
                item, COL4, target_index))
        except ValueError:
            col4_matched.append(data_col4[data_col3.index(item)])
            col5_matched.append(data_col5[data_col3.index(item)])
            logging.info('Not Found the item: {0} at the column: {1} and used the current item.'.format(
                item, COL4))

    for item in data_col2:
        try:
            col3_matched.append(data_col3[data_col3.index(item)])
        except ValueError:
            col3_matched.append(item)
          
            
    
    pd.DataFrame({
        COL1: data_col1,
        COL2: data_col2,
        COL3: col3_matched,
        COL4: col4_matched,
        COL5: col5_matched}).to_csv(
        BASE_FOLDER+CSV_FILE_NAME+RESULT+'-second-match'+CSV_FILE_TYPE,
        encoding=ENCODING_TYPE_UTF8,
        index=False
    )
    


if __name__ == '__main__':
    main()
