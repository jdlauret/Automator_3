import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
import time
import datetime as dt
from utilities.oracle_bridge import clear_table, update_table
from utilities.google_bridge import sheet_data

source_id = '1LLnbguhUtjTyGXq3MjHAP7jVPlvE_-YAQBA59JUYItM'
destination_id = ''
table_name = 'JDLAURET.T_ATTENDANCE_HOTLINE'

if __name__ == '__main__':

    today = dt.datetime.today().date()

    sheet_data = sheet_data(source_id, 'Form Responses 3')
    header = sheet_data[0]
    del sheet_data[0]

    dialed_time_index = header.index('Dialed Time')

    for i, row in enumerate(sheet_data):
        for j, cell in enumerate(row):
            if j == dialed_time_index:
                new_time = dt.datetime(*time.strptime(cell, '%I:%M:%S %p')[:6]).replace(day=today.day,
                                                                                        month=today.month,
                                                                                        year=today.year)
                sheet_data[i][j] = new_time

    try:
        clear_table(table_name)
    except:
        pass

    try:
        update_table(table_name, sheet_data, credentials='private', encoding='utf8', header_included=False)
    except:
        pass
