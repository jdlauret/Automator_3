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

import datetime as dt
import json
import os

import pandas
from BI.data_warehouse import SnowflakeV2, SnowflakeConnectionHandlerV2
from BI.utilities.incontact import InContactReport


class Upload:
    def __init__(self, report, table_name, clear_table=False):
        self.report = report
        self.table = table_name
        self.overwrite = clear_table

        self.upload_to_table()

    def upload_to_table(self):
        dw.insert_into_table(self.table, self.report, overwrite=self.overwrite)


class ReportDownloader:
    def __init__(self, download_log_key, **kwargs):
        self.download_log_key = download_log_key
        self.table_name = kwargs.get('table_name')
        self.cdst = kwargs.get('cdst')

        self.start_date = kwargs.get('start_date')
        self.end_date = kwargs.get('end_date')

        self.columns_to_keep = kwargs.get('columns_to_keep')
        self.date_columns = kwargs.get('date_columns')

        self.days_passed = (self.end_date - self.start_date).days
        self.upload_data = []

        self.columns = []
        self.data_frame = None
        self.file = 'download_dates.json'
        self.all_dates = None
        self.read_file()

        if self.download_log_key not in self.all_dates.keys():
            self.all_dates[self.download_log_key] = {}
        for i in range(self.days_passed + 1):
            new_key = str(self.start_date.date() + dt.timedelta(days=i))
            if new_key not in self.all_dates[self.download_log_key].keys():
                self.all_dates[self.download_log_key][new_key] = False

        self.write_file()

    def date_conversion(self, column, date_format):
        self.data_frame[column] = pandas.to_datetime(self.data_frame[column], format=date_format)

    def convert_timestamps(self, column):
        for j, row in enumerate(self.data_frame):
            for i, col in enumerate(row):
                if self.columns[i].lower() == column.lower():
                    self.data_frame[j][i] = col.to_pydatetime()

    def convert_to_list(self):
        self.columns = self.data_frame.columns.values.tolist()
        self.data_frame = self.data_frame.values.tolist()

    def read_file(self):
        with open(self.file, 'r') as infile:
            self.all_dates = json.load(infile)

    def write_file(self):
        with open(self.file, 'w') as outfile:
            json.dump(self.all_dates, outfile, indent=4)
        self.read_file()

    def get_all_data(self):
        for dict_key in self.all_dates[self.download_log_key].keys():
            start_date = dt.datetime.strptime(dict_key, '%Y-%m-%d')
            end_date = start_date.replace(hour=23, minute=59)
            if not self.all_dates[self.download_log_key][dict_key]:
                report = InContactReport(self.cdst, date_range=(start_date, end_date), preset_date=0)

                self.data_frame = pandas.DataFrame(report.report_results, columns=report.report_header)
                self.data_frame = self.data_frame[self.columns_to_keep]
                if self.date_columns:
                    for column_key in self.date_columns.keys():
                        date_format = self.date_columns[column_key]
                        self.date_conversion(column_key, date_format)

                self.convert_to_list()

                if self.date_columns:
                    for column_key in self.date_columns.keys():
                        self.convert_timestamps(column_key)

                if len(self.data_frame) > 0:
                    self.upload_data_to_table()
                self.all_dates[self.download_log_key][dict_key] = True
                self.write_file()

    def upload_data_to_table(self):
        Upload(self.data_frame, self.table_name)


reports_dict = {
    'agent_state_log': {
        'cdst': 'NfFAljNJS1cDGiZRzmGYsxJT0T3XDvMWdtWQK645kUKbQqAuc4WDpN%2bv%2fteRXSNeiBuq6BGqmrEhr5Es7CFkKapERYR'
                'qEJmuhqnkOXq05iYemNiK%2fXTXkMGYfItJ8m2iq1VCppKi5DP5n5a6%2bFaxCHuzmAgtLUaHc5A7i6XlwsqRmGKDxkqg8o'
                'foJNVpjz7rehoVPS8VaHr0a68%2fGv%2bXwUa4Yq8%3d',
        'start_date': dt.datetime(2017, 9, 1),
        'end_date': dt.datetime.today() - dt.timedelta(days=1),
        'table_name': 'D_POST_INSTALL.T_IC_AGENT_STATE_LOG',
        'columns_to_keep': [
            'Agent_No',
            'Start_Date',
            'Outstate',
            'End_Date',
            'Available',
        ],
        'date_columns': {
            'Start_Date': '%m/%d/%Y %I:%M:%S %p',
            'End_Date': '%m/%d/%Y %I:%M:%S %p',
        }
    },
    'skill_summary': {
        'cdst': 'NfFAljNJS1cDGiZRzmGYsxJT0T3XDvMWdtWQK645kUKX3DigiuQ9lK3gqhw3riK3gwXrht2QT2WGMOLfp6n4PfUKY6ltgsVxE'
                'QLfAJRzHQ3awPh6PBDrqU731GObPipPn4MU9%2bepnRKaA4t1kRSM2rhekAFpkA5BlwTvCmufSgFvcUMPyd9G6baumdVSeA%2'
                'buHzYwvN8z1WgCg5fWarPRkA%3d%3d',
        'start_date': dt.datetime(2017, 9, 1),
        'end_date': dt.datetime.today() - dt.timedelta(days=1),
        'table_name': 'D_POST_INSTALL.T_IC_SLA_HIST',
        'columns_to_keep': [
            'skill_no',
            'skill_name',
            'Campaign_Name',
            'Start_Date',
            'In_SLA',
            'Out_SLA',
        ],
        'date_columns': {
            'Start_Date': '%m/%d/%Y',
        }
    }
}

if __name__ == '__main__':
    current_dir = os.getcwd()
    os.chdir(os.path.dirname(os.path.realpath(__file__)))
    dw = SnowflakeV2(SnowflakeConnectionHandlerV2())
    dw.set_user('JDLAURET')
    dw.open_connection()
    try:
        for key in reports_dict.keys():
            rd = ReportDownloader(key, **reports_dict[key])
            rd.get_all_data()
            os.chdir(current_dir)
    finally:
        dw.close_connection()
