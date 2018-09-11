import os
import sys
import csv
import pandas as pd
import numpy as np
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from models import GDrive, SnowFlakeDW, SnowflakeConsole


FILE_ID = '1PFz4qrZp2x6hhgP1asac2Bk_O6G4Jz4Q'
FILE_NAME = 'Master WO Breakdown.csv'
TABLE_NAME = 'D_POST_INSTALL.T_WO_TELOGIS'


def telogis_data_from_excel(file_path):
    print('Converting CSV to Data Frame')
    df = pd.read_csv(file_path)
    print('Converting \'12:00:00 AM\' to NaN')
    df.loc[df['WO_TELOGIS_ARRIVAL'] == '12:00:00 AM', 'WO_TELOGIS_ARRIVAL'] = np.nan
    df.loc[df['WO_TELOGIS_DEPARTURE'] == '12:00:00 AM', 'WO_TELOGIS_DEPARTURE'] = np.nan
    df.loc[df['WO_WINDOW_FINISH'] == '1/0/00 0:00', 'WO_WINDOW_FINISH'] = np.nan
    print('Converting Zip Codes')
    df['WO_ZIP_CODE'] = df['WO_ZIP_CODE'].astype(str).str.zfill(5)
    print('Converting Window Start from string to date time')
    df['WO_WINDOW_START'] = pd.to_datetime(df['WO_WINDOW_START'])
    print('Converting Window Finish from string to date time')
    df['WO_WINDOW_FINISH'] = pd.to_datetime(df['WO_WINDOW_FINISH'])
    print('Converting Assignment Start from string to date time')
    df['WO_ASSIGNMENT_START'] = pd.to_datetime(df['WO_ASSIGNMENT_START'])
    print('Converting Assignment Finish from string to date time')
    df['WO_ASSIGNMENT_FINISH'] = pd.to_datetime(df['WO_ASSIGNMENT_FINISH'])
    print('Converting Telogis Arrival from string to date time')
    df['WO_TELOGIS_ARRIVAL'] = pd.to_datetime(df['WO_TELOGIS_ARRIVAL'])
    print('Converting Telogis Departure from string to date time')
    df['WO_TELOGIS_DEPARTURE'] = pd.to_datetime(df['WO_TELOGIS_DEPARTURE'])
    print('Converting Week from string to date')
    df['WO_WEEK'] = pd.to_datetime(df['WO_WEEK'])
    df = df.where((pd.notnull(df)), None)
    return df


if __name__ == '__main__':
    db = SnowFlakeDW()
    db.set_user('JDLAURET')
    db.open_connection()
    dw = SnowflakeConsole(db)
    print("Downloading Telogis Data")
    gd = GDrive()
    file = gd.download_file(FILE_ID, FILE_NAME)
    new_file = FILE_NAME.replace('.csv', '') + '_fixed.csv'
    try:
        with open(FILE_NAME, 'r', encoding='utf-8', errors='ignore') as infile, \
                open(new_file, 'w', encoding='utf-8') as outfile:

            inputs = csv.reader(infile)
            output = csv.writer(outfile)

            for index, row in enumerate(inputs):
                # Create file with no header
                output.writerow(row)

        try:
            telogis_data = telogis_data_from_excel(new_file)
            telogis_data.to_csv(new_file, sep=',', na_rep='', encoding='utf-8', index=False)
            df = pd.read_csv(new_file)
            df = df.where((pd.notnull(df)), None)
            df = df.values.tolist()
        except Exception as e:
            raise e

        try:
            dw.insert_into_table(TABLE_NAME, df, overwrite=True)
        except Exception as e:
            raise e
    finally:
        db.close_connection()
        os.remove(FILE_NAME)
        os.remove(new_file)
