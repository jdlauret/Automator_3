import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
import time
import datetime as dt
from models import SnowFlakeDW, SnowflakeConsole
from Archive.google_bridge import sheet_data

source_id = '1LLnbguhUtjTyGXq3MjHAP7jVPlvE_-YAQBA59JUYItM'
destination_id = ''
table_name = 'JDLAURET.T_ATTENDANCE_HOTLINE'

if __name__ == '__main__':
    db = SnowFlakeDW()
    db.set_user('JDLAURET')
    db.open_connection()
    dw = SnowflakeConsole(db)
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
        dw._clear_table(table=table_name)
    except:
        pass

    try:
        dw.insert_into_table(table_name, sheet_data)
    except:
        pass

    db.close_connection()
