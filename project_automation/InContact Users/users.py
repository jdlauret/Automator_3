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
        self._get_last_five_months()
        if self.console_output:
            print('Loading {url}'.format(url=REPORT_URL))
        # Go to Report URL
        self.driver.get(REPORT_URL)

        # Wait for download button to be present
        button_id = 'ctl00_ctl00_ctl00_ctl00_BaseContent_Content_ManagerContent_MultipleItemsWizardContent' \
                    '_btnDownloadExistingItems_ShadowButton'
        WebDriverWait(self.driver, self.delay).until(
            EC.presence_of_element_located((By.ID, button_id)))

        # Click Download Button
        download_button = self.driver.find_element_by_id(button_id)
        download_button.click()

        file_name = 'Users.csv'
        while not os.path.exists(os.path.join(DOWNLOAD_DIR, file_name)):
            if self.console_output:
                print('sleeping .1')
            sleep(.1)

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

    def __init__(self, last_five_months):
        self.data = []
        self.db = SnowflakeV2(SnowflakeConnectionHandlerV2())
        self.db.set_user('JDLAURET')
        self.last_five_months = last_five_months
        self.columns_to_keep = [
            'Agent ID',
            'First Name',
            'Last Name',
            'Custom 1',
            'Team ID',
            'Reports To ID',
            'Status',
        ]

    def process_new_files(self):
        self.db.open_connection()
        try:
            for file in os.listdir(DOWNLOAD_DIR):
                if file != 'desktop.ini':
                    file_name = os.path.basename(file)
                    self.process_file(os.path.join(DOWNLOAD_DIR, file_name))
            self.push_data_to_table()
        finally:
            self.db.close_connection()

    def process_file(self, file):
        data = pd.read_csv(file, header=0)
        data = data[self.columns_to_keep]
        data = data.values.tolist()
        self.data = self.data + data

    def push_data_to_table(self):
        if len(self.data) > 0:
            self.db.insert_into_table('D_POST_INSTALL.T_IC_USERS', self.data, overwrite=True)


def clear_downloads():
    for file in os.listdir(DOWNLOAD_DIR):
        if file != 'desktop.ini':
            os.unlink(os.path.join(DOWNLOAD_DIR, file))


if __name__ == '__main__':
    REPORT_URL = 'https://home-c20.incontact.com/inContact/Manage/ManageMultipleItems/' \
                 'ManageMultipleItems.aspx?type=Users'

    clear_downloads()
    crawler = PrimaryCrawler('chrome', download_directory=DOWNLOAD_DIR)
    try:
        crawler.run_crawler()

        processor = FileProcessor(crawler.last_five_months)
        processor.process_new_files()
    finally:
        if crawler.active:
            crawler.end_crawl()
