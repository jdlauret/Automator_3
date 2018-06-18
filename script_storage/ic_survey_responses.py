'''
CDR Plus Disposition Report == Daily
Base URL = https://home-c20.incontact.com/ReportService/DataDownloadHandler.ashx?

CDST = NfFAljNJS1cDGiZRzmGYsxJT0T3XDvMWdtWQK645kUKbQqAuc4WDpEHr3WjrJvFqvm26mxEoaO6IdIUJd8M5bbUB6ElDMFdtZsnHoCEu36
rztIUbvC%2bpqgI740v%2f82ODMcdRtg9Q%2fTwxDfDxACX9yynY40mXYOvPbdWa5aWoVQAhmRr%2fnEbvvblF%2fItav7jzzLurvT7n%2f1SxTsO
fWGGYnGrnCrc%3d

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
import math
import urllib.request

import pandas

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from automator import find_main_dir
from utilities.oracle_bridge import update_table, run_query

main_dir = find_main_dir()
storage_dir = os.path.join(main_dir, 'storage_files')


def cdr_report():
    base_url = 'https://home-c20.incontact.com/ReportService/DataDownloadHandler.ashx?'
    preset_date = True
    preset_date_range = '3'
    format = 'CSV'
    include_headers = 'True'
    append_date = 'True'
    cdst = "NfFAljNJS1cDGiZRzmGYsxJT0T3XDvMWdtWQK645kULC01jUhqQ8tEbL4y8XQadVU6iGocLtF5uQtJe%2bAkmkb9bhvzlYW7QMhhwQ" \
           "BvO93N79lLWHfFW7eTXygcWJTza5SV4s1S24vITA9f%2bg%2fQ7hs8MKdZ9F5ePEpvGD2666MVpy57OSupcLHYANN4wZnbea%2fb3F" \
           "yW9x%2fYVecpgg9eqw"
    url = base_url \
          + 'CDST=' \
          + cdst \
          + "&presetDate=" \
          + preset_date_range \
          + "&format=" \
          + format \
          + "&includeHeaders=" \
          + include_headers
    file_name = 'Survey_Responses.csv'
    os.chdir(storage_dir)

    print('Downloading InContact Data')
    urllib.request.urlretrieve(url, file_name)

    print('Formatting Data')
    df = pandas.read_csv(file_name)
    df = df[df.data_name != 'callrecordingid']

    df = df.values.tolist()

    data_object = {}

    for row in df:
        if row[0] not in data_object.keys():
            data_object[row[0]] = {}
        key = row[1]
        try:
            data_object[row[0]][key] = int(row[2])
        except:
            data_object[row[0]][key] = row[2]

    final_data_set = []

    for key in data_object.keys():
        current_obj = data_object[key]
        if 'nps_score' in current_obj.keys():
            if math.isnan(current_obj['nps_score']):
                nps_score = None
            else:
                nps_score = int(current_obj['nps_score'])
        else:
            nps_score = None

        if 'anps_score' in current_obj.keys():
            if math.isnan(current_obj['anps_score']):
                anps_score = None

            else:
                anps_score = int(current_obj['anps_score'])
        else:
            anps_score = None

        if 'issue_resolved' in current_obj.keys():
            if math.isnan(current_obj['issue_resolved']):
                issue_resolved = None
            elif current_obj['issue_resolved'] == 2:
                issue_resolved = 0
            else:
                issue_resolved = int(current_obj['issue_resolved'])
        else:
            issue_resolved = None
        new_row = [str(key), nps_score, anps_score, issue_resolved]
        if nps_score is not None  or anps_score is not None or issue_resolved is not None:
            final_data_set.append(new_row)

    query = """
    SELECT * FROM JDLAURET.T_IC_SURVEY_RESPONSES CDR
    """

    print('Getting Existing Contact ID\'s')
    existing_data = run_query('', raw_query=query, credentials='private')

    final_data_set = [x for x in final_data_set if x not in existing_data]

    if len(final_data_set) > 0:
        print('Sending to the Data Warehouse')
        update_table('JDLAURET.T_IC_SURVEY_RESPONSES', final_data_set, header_included=False, credentials='private')


if __name__ == '__main__':
    cdr_report()
