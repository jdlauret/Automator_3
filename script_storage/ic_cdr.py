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

import os
import sys
import urllib.request

import pandas

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from utilities.oracle_bridge import update_table, run_query

storage_dir = 'C:\\Users\\jonathan.lauret\\Google Drive\\Projects\\Python Projects\\Automator 3.0\\downloads'


def cdr_report():
    base_url = 'https://home-c20.incontact.com/ReportService/DataDownloadHandler.ashx?'
    preset_date = True
    preset_date_range = '3'
    format = 'CSV'
    include_headers = 'True'
    append_date = 'True'
    cdst = "NfFAljNJS1cDGiZRzmGYsxJT0T3XDvMWdtWQK645kUKbQqAuc4WDpEHr3WjrJvFqvm26mxEoaO6IdIUJd8M5bbUB6ElDMFdtZsnHoCEu36"\
           "rztIUbvC%2bpqgI740v%2f82ODMcdRtg9Q%2fTwxDfDxACX9yynY40mXYOvPbdWa5aWoVQAhmRr%2fnEbvvblF%2fItav7jzzLurvT7n%2"\
           "f1SxTsOfWGGYnGrnCrc%3d"
    url = base_url + 'CDST=' + cdst \
          + "&presetDate=" + preset_date_range \
          + "&format=" + format + "&includeHeaders=" \
          + include_headers + "&appendDate=" + append_date
    file_name = 'CDR Plus Disposition.csv'
    os.chdir(storage_dir)

    print('Downloading InContact Data')
    urllib.request.urlretrieve(url, file_name)

    print('Formatting Data')
    df = pandas.read_csv(file_name)
    df['Start_Date'] = df['Start_Date'] + ' ' + df['start_time']
    del df['start_time']

    data = df.values.tolist()

    query = """
    SELECT CDR.CONTACT_ID FROM SOLAR.T_IC_CDR CDR WHERE CDR.START_DATE >= TRUNC(SYSDATE) - 7
    """

    print('Getting Existing Contact ID\'s')
    contact_id_list = run_query('', '', raw_query=query)
    contact_id_list = [x[0] for x in contact_id_list]
    del contact_id_list[0]

    lines_for_removal = []
    print('Removing Duplicates')
    for i, contact_id in enumerate(data):
        if str(contact_id[0]) in contact_id_list:
            lines_for_removal.append(i)
    lines_removed = 0
    for remove in list(reversed(lines_for_removal)):
        lines_removed += 1
        del data[remove]
    print('Removed {0} duplicate(s) from data set'.format(lines_removed))
    print('Sending to the Data Warehouse')
    update_table('SOLAR.T_IC_CDR', data, header_included=False, date_time=True)


if __name__ == '__main__':
    cdr_report()
