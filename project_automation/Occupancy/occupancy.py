import csv
import datetime as dt
import json
import os
from time import sleep

from BI.data_warehouse import SnowflakeV2, SnowflakeConnectionHandlerV2
from BI.web_crawler import CrawlerBase
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

MAIN_DIR = os.path.dirname(os.path.realpath(__file__))
DOWNLOAD_DIR = os.path.join(MAIN_DIR, 'downloads')
LOGS_DIR = os.path.join(MAIN_DIR, 'logs')


class PrimaryCrawler(CrawlerBase):
    """
    Primary Crawler is a web crawler that will login to the Login URL
    """
    def __init__(self, driver_type, download_directory=None, headless=False):
        # Primary Items
        CrawlerBase.__init__(self, driver_type, download_directory=download_directory, headless=None)

        self.download_file_path = os.path.join(LOGS_DIR, 'report_download.json')
        self.downloads = {}
        self.read_download_file()

    def read_download_file(self):
        with open(self.download_file_path) as f:
            self.downloads = json.load(f)

    def write_download_file(self, date):
        date_str = date.strftime('%Y-%m-%d')
        self.downloads[date_str] = True
        with open(self.download_file_path, 'w') as outfile:
            json.dump(self.downloads, outfile, indent=4)
        self.read_download_file()

    def get_hour_start_end(self, date):
        return date.strftime("%I:00:00 %p"), date.strftime("%I:59:59 %p")

    def get_report(self):
        if self.console_output:
            print('Loading {url}'.format(url=REPORT_URL))
        # Go to Report URL
        self.driver.get(REPORT_URL)

        # Read Through Dates in Downloads Json
        for date_str in self.downloads.keys():
            # Set date to date_str and hour to hour
            date = dt.datetime.strptime(date_str, '%Y-%m-%d')
            # Get Start and End Time as strings
            start_time_str, end_time_str = self.get_hour_start_end(date)
            if not self.downloads[date_str]:
                WebDriverWait(self.driver, self.delay) \
                    .until(EC.presence_of_element_located(
                    (By.ID, 'ctl00_ctl00_ctl00_BaseContent_Content_ManagerContent_ReportTemplateTabContainer_'
                            'ReportTemplateDetailsPanel_btnRunReport_ShadowButton')
                ))

                # Click Run
                self.driver.find_element_by_id('ctl00_ctl00_ctl00_BaseContent_Content_ManagerContent_'
                                               'ReportTemplateTabContainer_ReportTemplateDetailsPanel_'
                                               'btnRunReport_ShadowButton').click()

                # Click Download Button
                while not os.path.isfile(os.path.join(DOWNLOAD_DIR, 'Handle Time Report.csv')):
                    if self.console_output:
                        print('sleeping .1')
                    sleep(.1)
                new_file_name = date_str
                self.rename_file(new_file_name)
                self.write_download_file(date)

    def rename_file(self, date_str):
        for file in os.listdir(DOWNLOAD_DIR):
            if file.lower() == 'handle time report.csv':
                file_path = os.path.join(DOWNLOAD_DIR, file)
                file_name = date_str.replace('/', '-') + ' ' + os.path.basename(file_path)
                new_file_path = os.path.join(DOWNLOAD_DIR, file_name)
                os.rename(file_path, new_file_path)

    def run_crawler(self):
        payload = {
            'url': 'https://login.incontact.com/inContact/Login.aspx',
            'username': {'input': os.environ.get('MACK_EMAIL'), 'id': 'ctl00_BaseContent_tbxUserName', },
            'password': {'input': os.environ.get('INCONTACT_PASS'), 'id': 'ctl00_BaseContent_tbxPassword', },
            'submit': {'id': 'ctl00_BaseContent_btnLogin', },
        }
        # Login to page
        self.login(**payload)
        sleep(2)
        # Call URL
        if self.console_output:
            print(self.driver.current_url)
        self.get_report()

        # Shutdown the Crawler
        self.end_crawl()


class FileProcessor:

    def __init__(self):
        self.data_header = []
        self.data = []
        self.db = SnowflakeV2(SnowflakeConnectionHandlerV2())
        self.db.set_user('JDLAURET')
        self.dw = None
        self.process_file_path = os.path.join(LOGS_DIR, 'Processed Files.txt')
        self.processed_files = []

    def process_new_files(self):
        self.read_process_file()
        for file in os.listdir(DOWNLOAD_DIR):
            if file != 'desktop.ini':
                file_name = os.path.basename(file)
                if file_name not in self.processed_files:
                    self.process_file(file_name)
                    self.write_process_file(file_name)

    def read_process_file(self):
        with open(self.process_file_path) as f:
            self.processed_files = f.read().split('\n')

    def write_process_file(self, string):
        with open(self.process_file_path, 'a') as outfile:
            outfile.write(string + '\n')
        with open(self.process_file_path) as infile:
            self.processed_files = infile.read().split('\n')

    def process_file(self, file):
        with open(os.path.join(DOWNLOAD_DIR, file)) as f:
            reader = csv.reader(f)
            data = [r for r in reader]
        if len(data) > 0:
            column_names = self.data_header = data[0]
            del data[0:3]
            if column_names[0] != '':
                self.data = data
                self.remove_blank_rows()
                self.trim_extra_column()
                self.push_data_to_table()

    def add_date(self):
        yesterday = dt.datetime.today().date() - dt.timedelta(days=1)
        for i, row in enumerate(self.data):
            self.data[i].append(yesterday)

    def remove_blank_rows(self):
        new_list = []
        for i, row in enumerate(self.data):
            if row:
                new_list.append(row)
        self.data = new_list

    def trim_extra_column(self):
        for i, row in enumerate(self.data):
            if len(row) > 4:
                self.data[i] = row[:4]

    def push_data_to_table(self):
        if len(self.data) > 0:
            self.db.open_connection()
            try:
                self.db.insert_into_table('D_POST_INSTALL.T_IC_OCCUPANCY', self.data)
            finally:
                self.db.close_connection()


def generate_json(file_path, today):
    with open(file_path) as f:
        data_dict = json.load(f)

    date_str = today.strftime('%Y-%m-%d')
    if date_str not in data_dict.keys():
        data_dict[date_str] = False

    with open(file_path, 'w') as outfile:
        json.dump(data_dict, outfile, indent=4)


if __name__ == '__main__':
    REPORT_URL = 'https://home-c20.incontact.com/inContact/Manage/Reports/' \
                 'CustomReporting/ReportTemplateDetails.aspx?Id=7805'

    TODAY = dt.datetime.now()

    generate_json(os.path.join(LOGS_DIR, 'report_download.json'), TODAY)

    crawler = PrimaryCrawler('chrome', download_directory=DOWNLOAD_DIR)
    try:
        crawler.run_crawler()

        processor = FileProcessor()
        processor.process_new_files()
    finally:
        if crawler.active:
            crawler.end_crawl()
