'''
CDR Plus Disposition Report == Daily
Base URL = https://home-c20.incontact.com/ReportService/DataDownloadHandler.ashx?

CDST = NfFAljNJS1cDGiZRzmGYsxJT0T3XDvMWdtWQK645kUKbQqAuc4WDpEHr3WjrJvFqvm26mxEoaO6IdIUJd8M5bbUB6ElDMFdtZsnHoCEu36rztIUbvC%2bpqgI740v%2f82ODMcdRtg9Q%2fTwxDfDxACX9yynY40mXYOvPbdWa5aWoVQAhmRr%2fnEbvvblF%2fItav7jzzLurvT7n%2f1SxTsOfWGGYnGrnCrc%3d

PresetDate = # from the list below
1 = Today
2 = Yesterday
3 = Last 7 Days
4 = Last 30 Days
5 = Previous Week
6 = Previous Month
7 = Month to Date

DateFrom = (Date) 12%2f27%2f2017 + (Time) 7%3a00%3a00 + AM
DateTo = (Date) 12%2f28%2f2017 + (Time) 6%3a59%3a00 + AM

Format = CSV
IncludeHeaders = True
AppendDate = True
'''

import urllib.request
import os
import sys
import pandas
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from automator import find_main_dir
from utilities.oracle_bridge import update_table, clear_table

main_directory = find_main_dir()
storage_dir = os.path.join(main_directory, 'storage_files')


def cdr_report():
    base_url = 'https://home-c20.incontact.com/ReportService/DataDownloadHandler.ashx?'
    preset_date = True
    preset_date_range = '1'
    format = 'CSV'
    include_headers = 'True'
    append_date = 'True'
    cdst = "NfFAljNJS1cDGiZRzmGYsxJT0T3XDvMWdtWQK645kUKbQqAuc4WDpGkjUmRJC76IgA9IBQEJX4fmbIkEYBnGa83216ZRasLh%2bOfb6L%"\
           "2b3oEDcaOwVbVEzzGQ4sKuD35%2fNzGkk7gXl9yl1BFOnUi%2fk4vROW%2fsIlVID6w%2fobaKsZ7i5YKvClharzMd4XF%2fAhGG5G13K"\
           "bybvB13TiRBKyX2D8Xm5hKQ%3d"
    os.chdir('..')
    url = base_url + 'CDST=' + cdst\
        + "&presetDate=" + preset_date_range\
        + "&format=" + format + "&includeHeaders="\
        + include_headers + "&appendDate=" + append_date
    file_name = 'Agent List.csv'
    file_location = storage_dir
    file_path = os.path.join(file_location, file_name)

    print('Downloading InContact Data')
    urllib.request.urlretrieve(url, file_path)

    print('Formatting Data')
    df = pandas.read_csv(file_path)
    del df['last_login']
    data = df.values.tolist()

    print('Removing existing data in Data Warehouse')
    clear_table('SOLAR.T_IC_AGENT_LIST')
    print('Sending to the Data Warehouse')
    update_table('SOLAR.T_IC_AGENT_LIST', data, header_included=False)


if __name__ == '__main__':
    cdr_report()
