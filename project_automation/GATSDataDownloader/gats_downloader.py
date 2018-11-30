import datetime as dt
import glob
import json
import os
from time import sleep

import pandas as pd
from dateutil.relativedelta import relativedelta
from BI.data_warehouse.connector import Snowflake
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

CHROME_DRIVER_PATH = r'C:\\Users\\jonathan.lauret\\Google Drive\\Projects\\Chrome Driver\chromedriver.exe'
FIREFOX_DRIVER_PATH = r'C:\\Users\\jonathan.lauret\\Google Drive\\Projects\\Chrome Driver\geckodriver.exe'

MAIN_DIR = os.path.dirname(os.path.realpath(__file__))
DOWNLOAD_DIR = os.path.join(MAIN_DIR, 'downloads')
LOGS_DIR = os.path.join(MAIN_DIR, 'logs')


class PrimaryCrawler:
    """
    Primary Crawler is a web crawler that will login to the Login URL
    """
    delay = 10
    num_reports = 3
    crawler_log = {
        'login': False,
        'number_of_downloads': 0
    }

    def __init__(self, login_payload, driver, testing=False):
        # Primary Items
        self.testing = testing
        self.skip = False
        self.driver = driver

        self.download_file_path = os.path.join(LOGS_DIR, 'report_download.json')
        self.payload = login_payload
        self.testing = testing
        self.last_five_months = []
        self.number_months_back = 5

        self.fp = webdriver.FirefoxProfile()

        if self.testing:
            print('Testing is active')

        if self.driver.lower() == 'firefox':
            self.options = webdriver.FirefoxOptions()
            self.setup_firefox_driver()
            # Create the Driver and Delete all cookies
            self.DRIVER = webdriver.Firefox(executable_path=FIREFOX_DRIVER_PATH,
                                            options=self.options,
                                            firefox_profile=self.fp)
        elif self.driver.lower() == 'chrome':
            self.options = webdriver.ChromeOptions()
            self.setup_chrome_driver()
            self.DRIVER = webdriver.Chrome(executable_path=CHROME_DRIVER_PATH,
                                           options=self.options)

        self.DRIVER.delete_all_cookies()

    def setup_firefox_driver(self):

        # Default Preference Change stops Fingerprinting on site
        self.fp.DEFAULT_PREFERENCES['frozen']['dom.disable_open_during_load'] = True
        # Set the Download preferences to change the download folder location and to auto download reports
        self.fp.set_preference('browser.download.folderList', 2)
        self.fp.set_preference('browser.download.manager.showWhenStarting', False)
        self.fp.set_preference('browser.download.dir', DOWNLOAD_DIR)
        self.fp.set_preference('browser.helperApps.neverAsk.saveToDisk', 'application/vnd.ms-excel')

        if not self.testing:
            # Set driver to headless when not in testing mode
            self.options.add_argument('--headless')

    def setup_chrome_driver(self):
        prefs = {
            'profile.default_content_settings.popups': 0,
            'download.default_directory': DOWNLOAD_DIR + '\\',
            'directory_upgrade': True,
        }
        self.options.add_experimental_option('prefs', prefs)
        self.options.add_argument("disable-infobars")

        # if not self.testing:
        #     self.options.add_argument('headless')

    def read_download_file(self):
        with open(self.download_file_path) as f:
            self.downloads = json.load(f)

    def write_download_file(self, date):
        date_str = date.strftime('%Y-%m-%d')
        self.downloads[date_str] = True
        with open(self.download_file_path, 'w') as outfile:
            json.dump(self.downloads, outfile, indent=4)
        self.read_download_file()

    def login(self):
        # Use Driver to login
        # Go to Login Page
        print('Logging into {url}'.format(url=LOGIN_URL))
        self.DRIVER.get(LOGIN_URL)

        WebDriverWait(self.DRIVER, self.delay) \
            .until(EC.presence_of_element_located((By.ID, self.payload['username']['html_name'])))
        # Insert Username
        uname = self.DRIVER.find_element_by_id(self.payload['username']['html_name'])
        uname.send_keys(self.payload['username']['value'])

        # Insert Password
        passw = self.DRIVER.find_element_by_id(self.payload['password']['html_name'])
        passw.send_keys(self.payload['password']['value'])

        # Click the Login button
        login_button = self.payload['button']['id']
        self.DRIVER.find_element_by_id(login_button).click()
        self.crawler_log['login'] = True

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
            print('Loading {url}'.format(url=REPORT_URL))
            # Go to Report URL
            self.DRIVER.get(REPORT_URL)

            # Read Through Dates in Downloads Json
            WebDriverWait(self.DRIVER, self.delay).until(
                EC.presence_of_element_located((By.ID, submit_name)))
            input = self.DRIVER.find_element_by_id(input_name)
            input.click()
            input.send_keys(Keys.CONTROL, 'a')
            input.send_keys(value)
            self.DRIVER.find_element_by_id(submit_name).click()

            # Click Download Button
            csv_button = 'CSV_CD'
            WebDriverWait(self.DRIVER, self.delay).until(EC.presence_of_element_located((By.ID, csv_button)))
            self.DRIVER.find_element_by_id(csv_button).click()
            temp_file_name = 'AccountSolarGeneration*.csv'
            while not glob.glob(os.path.join(DOWNLOAD_DIR, temp_file_name)):
                if self.testing:
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
        # Login to page
        self.login()
        sleep(2)
        # Call URL
        if self.testing:
            print(self.DRIVER.current_url)
        self.get_report()

        # Shutdown the Crawler
        self.end_crawl()

    def end_crawl(self):
        # Close the Driver
        print('Crawler Tasks Complete')
        self.DRIVER.quit()


class FileProcessor:

    def __init__(self, last_five_months):
        self.data = []
        self.db = Snowflake()
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
        for file in os.listdir(DOWNLOAD_DIR):
            if file != 'desktop.ini':
                file_name = os.path.basename(file)
                self.process_file(os.path.join(DOWNLOAD_DIR, file_name))
        self._clear_existing_data()
        self.push_data_to_table()

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
        self.db.open_connection()
        try:
            self.db.execute_sql_command(query)
        finally:
            self.db.close_connection()

    def push_data_to_table(self):
        if len(self.data) > 0:
            try:
                self.db.insert_into_table('D_POST_INSTALL.T_GATS_GEN', self.data)
            finally:
                self.db.close_connection()


def clear_downloads():
    for file in os.listdir(DOWNLOAD_DIR):
        if file != 'desktop.ini':
            os.unlink(os.path.join(DOWNLOAD_DIR, file))


if __name__ == '__main__':
    LOGIN_URL = 'https://gats.pjm-eis.com/gats2/Login/Index/'
    REPORT_URL = 'https://gats.pjm-eis.com/gats2/AccountReports/AccountSolarGeneration'

    payload = {
        'username': {
            'html_name': 'loginName',
            'value': os.environ.get('GATS_USERNAME')
        },
        'password': {
            'html_name': 'password',
            'value': os.environ.get('GATS_PASS')
        },
        'button': {
            'id': 'submitLogin'
        }
    }

    clear_downloads()

    try:
        crawler = PrimaryCrawler(payload, 'chrome')
        crawler.run_crawler()

        processor = FileProcessor(crawler.last_five_months)
        processor.process_new_files()
    except Exception:
        crawler.end_crawl()
