import datetime as dt
import glob
import json
import os
from time import sleep

import pandas as pd
from dateutil.relativedelta import relativedelta
from BI.data_warehouse import SnowflakeV2, SnowflakeConnectionHandlerV2
from BI.web_crawler import CrawlerBase
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
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
        self.last_five_months = []
        self.number_months_back = 5

    def _get_last_five_months(self):
        today = dt.date.today()
        month_year_list = []
        for i in range(self.number_months_back):
            previous_month = (today - relativedelta(months=i + 1))
            month = previous_month.month
            if len(str(month)) < 2:
                month = '0' + str(month)
            month_year = previous_month.year
            self.last_five_months.append(str(month) + '/' + str(month_year))

    def get_report(self):
        input_name = 'SelectedTimeID_I'
        submit_name = 'SubmitButton'
        self._get_last_five_months()
        for value in self.last_five_months:
            if self.console_output:
                print('Loading {url}'.format(url=REPORT_URL))
            # Go to Report URL
            self.driver.get(REPORT_URL)

            # Read Through Dates in Downloads Json
            WebDriverWait(self.driver, self.delay).until(
                EC.presence_of_element_located((By.ID, submit_name)))
            input = self.driver.find_element_by_id(input_name)
            input.click()
            input.send_keys(Keys.CONTROL, 'a')
            input.send_keys(value)
            self.driver.find_element_by_id(submit_name).click()

            # Click Download Button
            csv_button = 'CSV_CD'
            WebDriverWait(self.driver, self.delay).until(EC.presence_of_element_located((By.ID, csv_button)))
            self.driver.find_element_by_id(csv_button).click()
            temp_file_name = 'AccountSolarGeneration*.csv'
            while not glob.glob(os.path.join(DOWNLOAD_DIR, temp_file_name)):
                if self.console_output:
                    print('sleeping .1')
                sleep(.1)
            new_file_name = (value + '.csv').replace('/', '-')
            self._rename_file(new_file_name)

    def _rename_file(self, new_file_name):
        for file in os.listdir(DOWNLOAD_DIR):
            if file != 'desktop.ini':
                if 'AccountSolarGeneration' in file:
                    os.rename(os.path.join(DOWNLOAD_DIR, file), os.path.join(DOWNLOAD_DIR, new_file_name))

    def run_crawler(self):
        payload = {
            'url': 'https://gats.pjm-eis.com/gats2/Login/Index/',
            'username': {'input': os.environ.get('GATS_USERNAME'), 'id': 'loginName', },
            'password': {'input': os.environ.get('GATS_PASS'), 'id': 'password', },
            'submit': {'id': 'submitLogin', },
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

    def __init__(self, last_five_months):
        self.data = []
        db_connection = SnowflakeConnectionHandlerV2()
        self.db = SnowflakeV2(db_connection)
        self.db.set_user('JDLAURET')
        self.last_five_months = last_five_months
        self.columns_to_keep = [
            'Month of Generation',
            'GATS Gen ID',
            'Facility Name',
            'Generation (kWh)',
            'Generation Owed (kWh)',
            'Carryover (kWh)',
            'MMA (kWh)',
            'Adjusted Generation (kWh)',
            'Remaining Generation (kWh)',
            'Meter ID',
            'Meter Name',
            'Last Meter Read (kWh/Btu)',
            'Last Meter Read Date',
        ]

    def process_new_files(self):
        self.db.open_connection()
        try:
            for file in os.listdir(DOWNLOAD_DIR):
                if file != 'desktop.ini':
                    file_name = os.path.basename(file)
                    self.process_file(os.path.join(DOWNLOAD_DIR, file_name))
            self._clear_existing_data()
            self.push_data_to_table()
        finally:
            self.db.close_connection()

    def process_file(self, file):
        data = pd.read_csv(file, header=0)
        data = data[self.columns_to_keep]
        data = data.values.tolist()
        self.data = self.data + data

    def _clear_existing_data(self):
        latest_date = self.last_five_months[-1]
        month, year = latest_date.split('/')

        query = """
        DELETE FROM D_POST_INSTALL.T_GATS_GEN gg
        WHERE gg.MONTH_OF >= TO_DATE('{year}-{month}-01', 'YYYY-MM-DD')
        OR gg.MONTH_OF IS NULL
        """.format(year=year, month=month)
        self.db.execute_sql_command(query)

    def push_data_to_table(self):
        if len(self.data) > 0:
            self.db.insert_into_table('D_POST_INSTALL.T_GATS_GEN', self.data)


def clear_downloads():
    for file in os.listdir(DOWNLOAD_DIR):
        if file != 'desktop.ini':
            os.unlink(os.path.join(DOWNLOAD_DIR, file))


if __name__ == '__main__':
    REPORT_URL = 'https://gats.pjm-eis.com/gats2/AccountReports/AccountSolarGeneration'

    clear_downloads()
    crawler = PrimaryCrawler('chrome', download_directory=DOWNLOAD_DIR)
    try:
        crawler.run_crawler()

        processor = FileProcessor(crawler.last_five_months)
        processor.process_new_files()
    finally:
        if crawler.active:
            crawler.end_crawl()
