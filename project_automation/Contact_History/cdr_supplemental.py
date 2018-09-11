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
from models import DataWarehouse, SnowFlakeDW, SnowflakeConsole

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
        self.downloads = {}
        self.read_download_file()
        self.payload = login_payload
        self.testing = testing

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
            self.options = Options()
            self.setup_chrome_driver()
            self.DRIVER = webdriver.Chrome(executable_path=CHROME_DRIVER_PATH,
                                           chrome_options=self.options)

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

        if not self.testing:
            # self.options.add_argument('headless')
            self.options.add_argument('--disable-gpu')

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

    def get_report(self):
        print('Loading {url}'.format(url=REPORT_URL))
        # Go to Report URL
        self.DRIVER.get(REPORT_URL)

        WebDriverWait(self.DRIVER, self.delay) \
            .until(EC.presence_of_element_located(
            (By.ID, 'ctl00_ctl00_ctl00_BaseContent_Content_ManagerContent_ReportTemplateTabContainer_'
                    'ReportTemplateDetailsPanel_btnRunReport_ShadowButton')
        ))
        # Click Run
        self.DRIVER \
            .find_element_by_id('ctl00_ctl00_ctl00_BaseContent_Content_ManagerContent_ReportTemplateTabContainer_'
                                'ReportTemplateDetailsPanel_btnRunReport_ShadowButton') \
            .click()

        while not os.path.isfile(os.path.join(DOWNLOAD_DIR, 'CDR Supplemental.CSV')):
            if self.testing:
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
        self.DRIVER.close()


class FileProcessor:

    def __init__(self):
        self.data_header = []
        self.data = []
        self.dw = DataWarehouse('admin')
        self.sfdw = SnowflakeConsole(SFDB)
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
        self.sfdw.insert_into_table('D_POST_INSTALL.' + table_name, self.data, date_time_format='%Y/%m/%d %H:%M:%S')
        self.dw.insert_data_to_table('JDLAURET.' + table_name, self.data)


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
    LOGIN_URL = 'https://login.incontact.com/inContact/Login.aspx?ReturnUrl=%2f'
    REPORT_URL = 'https://home-c20.incontact.com/inContact/Manage/Reports' \
                 '/CustomReporting/ReportTemplateDetails.aspx?Id=7252#'

    TODAY = dt.datetime.now()
    SFDB = SnowFlakeDW()
    SFDB.set_user('JDLAURET')
    SFDB.print_message_on()
    SFDB.open_connection()
    generate_json(os.path.join(LOGS_DIR, 'report_download.json'), TODAY)

    payload = {
        'username': {
            'html_name': 'ctl00_BaseContent_tbxUserName',
            'value': os.environ.get('INCONTACT_USERNAME')
        },
        'password': {
            'html_name': 'ctl00_BaseContent_tbxPassword',
            'value': os.environ.get('INCONTACT_PASSWORD')
        },
        'button': {
            'id': 'ctl00_BaseContent_btnLogin'
        }
    }
    crawler = PrimaryCrawler(payload, 'chrome')
    crawler.run_crawler()

    processor = FileProcessor()
    processor.process_new_files()
    SFDB.close_connection()
