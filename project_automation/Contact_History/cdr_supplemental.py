import csv
import datetime as dt
import json
import os
import re
from time import sleep

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from BI.data_warehouse import SnowflakeV2, SnowflakeConnectionHandlerV2
from BI.web_crawler import CrawlerBase

MAIN_DIR = os.path.dirname(os.path.realpath(__file__))
DOWNLOAD_DIR = os.path.join(MAIN_DIR, 'downloads')
LOGS_DIR = os.path.join(MAIN_DIR, 'logs')


class PrimaryCrawler(CrawlerBase):

    def __init__(self, driver, download_directory=None, headless=False):
        CrawlerBase.__init__(self, driver, download_directory, headless)

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

    def get_report(self):
        if self.console_output:
            print('Loading {url}'.format(url=REPORT_URL))
        # Go to Report URL
        self.driver.get(REPORT_URL)

        WebDriverWait(self.driver, self.delay) \
            .until(EC.presence_of_element_located(
            (By.ID, 'ctl00_ctl00_ctl00_BaseContent_Content_ManagerContent_ReportTemplateTabContainer_'
                    'ReportTemplateDetailsPanel_btnRunReport_ShadowButton')
        ))
        # Click Run
        self.driver \
            .find_element_by_id('ctl00_ctl00_ctl00_BaseContent_Content_ManagerContent_ReportTemplateTabContainer_'
                                'ReportTemplateDetailsPanel_btnRunReport_ShadowButton') \
            .click()

        while not os.path.isfile(os.path.join(DOWNLOAD_DIR, 'CDR Supplemental.CSV')):
            if self.console_output:
                print('sleeping .1')
            sleep(.1)
        date = dt.datetime.today().date() - dt.timedelta(days=1)
        date_str = date.strftime('%Y-%m-%d')
        new_file_name = date_str
        self.rename_file(new_file_name)
        self.write_download_file(date)

    def rename_file(self, date_str):
        for file in os.listdir(DOWNLOAD_DIR):
            if file == 'CDR Supplemental.CSV':
                file_path = os.path.join(DOWNLOAD_DIR, file)
                file_name = date_str + ' ' + os.path.basename(file_path)
                new_file_path = os.path.join(DOWNLOAD_DIR, file_name)
                os.rename(file_path, new_file_path)

    def run_crawler(self):
        date = dt.datetime.today().date() - dt.timedelta(days=1)
        date_str = date.strftime('%Y-%m-%d')
        if not self.downloads[date_str]:
            # Login to page
            payload = {
                'url': 'https://login.incontact.com/inContact/Login.aspx',
                'username': {'input': os.environ.get('MACK_EMAIL'), 'id': 'ctl00_BaseContent_tbxUserName', },
                'password': {'input': os.environ.get('INCONTACT_PASS'), 'id': 'ctl00_BaseContent_tbxPassword', },
                'submit': {'id': 'ctl00_BaseContent_btnLogin', },
            }
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
        db_connection = SnowflakeConnectionHandlerV2()
        self.sfdw = SnowflakeV2(db_connection)
        self.sfdw.set_user('JDLAURET')
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
            reader = csv.reader(f, dialect='excel')
            data = [r for r in reader]
        if len(data) > 0:
            self.data_header = data[0]
            del data[0:2]
            self.data = data
            self.trim_data()
            self.push_data_to_table()

    def find_date_col(self):
        for i, col in enumerate(self.data_header):
            formatted_string = re.sub(r'[^A-Za-z]', '', col)
            if formatted_string.lower() == 'date':
                return i + 1
        return 0

    def trim_data(self):
        new_data = []
        first_col = self.find_date_col()
        last_col = self.data_header.index('Contact Duration') + 1
        for i, row in enumerate(self.data):
            if len(row) > 7 and row[0] != '':
                new_data.append(row[first_col:last_col])
        self.data = new_data

    def push_data_to_table(self):
        table_name = 'T_IC_CDR_SUPPLEMENTAL'
        try:
            self.sfdw.insert_into_table('D_POST_INSTALL.' + table_name, self.data, date_time_format='%Y/%m/%d %H:%M:%S')
        finally:
            self.sfdw.close_connection()


def generate_json(file_path, date):
    with open(file_path) as f:
        data_dict = json.load(f)

    date = date - dt.timedelta(days=1)
    date_str = date.strftime('%Y-%m-%d')
    if date_str not in data_dict.keys():
        data_dict[date_str] = False

    with open(file_path, 'w') as outfile:
        json.dump(data_dict, outfile, indent=4)


if __name__ == '__main__':
    REPORT_URL = 'https://home-c20.incontact.com/inContact/Manage/Reports' \
                 '/CustomReporting/ReportTemplateDetails.aspx?Id=7252#'

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
