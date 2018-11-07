import datetime as dt
import json
import os
from time import sleep

import pandas as pd
from BI.data_warehouse.connector import Snowflake
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait, Select

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

    def _current_quarter(self):
        today = dt.datetime.today()
        quarter_list = [(m - 1) // 3 + 1 for m in range(1, 13)]
        return quarter_list[today.month - 1], today.year

    def _create_quarter_year_list(self):
        #  Get current quarter and year
        #  Go back 1 quarter
        #  If quarter is equal to zero, reset to Q4 and roll the year back 1
        quarter_year_list = []
        current_quarter, current_year = self._current_quarter()
        quarter_year_list.append((current_quarter, current_year))
        next_quarter = current_quarter
        next_year = current_year
        for i in range(2):
            next_quarter = next_quarter - 1
            if next_quarter < 1:
                next_quarter = 4
                next_year = next_year - 1
            quarter_year_list.append((next_quarter, next_year))
        self.quarter_year_list = quarter_year_list
        return quarter_year_list

    def get_report(self):
        print('Loading {url}'.format(url=REPORT_URL))
        # Go to Report URL
        self.DRIVER.get(REPORT_URL)

        # Read Through Dates in Downloads Json
        WebDriverWait(self.DRIVER, self.delay).until(
            EC.presence_of_element_located((By.ID, 'ctl00_cphBody_btnIndividualSystems_CSV')))

        quarter_drop_down = 'ctl00$cphBody$ddlIndividualSystems_Quarter'
        year_drop_down = 'ctl00$cphBody$ddlIndividualSystems_Year'
        for quarter, year in self._create_quarter_year_list():
            quarter_string = 'Q' + str(quarter)
            year_string = str(year)
            self.DRIVER.find_element_by_id('Skinnedctl00_cphBody_ddlIndividualSystems_Quarter').click()
            quarter_select = Select(self.DRIVER.find_element_by_xpath("//select[@name=\"" + quarter_drop_down + "\"]"))
            quarter_select.select_by_value(str(quarter))
            year_select = Select(self.DRIVER.find_element_by_xpath("//select[@name=\'" + year_drop_down + "\']"))
            year_select.select_by_value(str(year))

            # Click Download Button
            self.DRIVER.find_element_by_id('ctl00_cphBody_btnIndividualSystems_CSV').click()
            file_name_1 = 'IndvSystemsProductionPreliminary ' + year_string + ' ' + quarter_string + '.csv'
            file_name_2 = 'IndvSystemsProductionFinal ' + year_string + ' ' + quarter_string + '.csv'
            while not os.path.isfile(os.path.join(DOWNLOAD_DIR, file_name_1)) \
                    and not os.path.isfile(os.path.join(DOWNLOAD_DIR, file_name_2)):
                if self.testing:
                    print('sleeping .1')
                sleep(.1)

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

    def __init__(self, quarter_year_list):
        self.data = []
        self.db = Snowflake()
        self.db.set_user('JDLAURET')
        self.quarter_year_list = quarter_year_list
        self.quarter_strings = ['Q' + str(x[0]) + '-' + str(x[1]) for x in self.quarter_year_list]
        self.columns_to_keep = [
            "SystemID",
            "SREC_AgreementSigned",
            "SREC_OptinStartDate1",
            "SREC_EffectiveDate1",
            "dateinservice",
            "EnergyProduced",
            "PeriodEndDate",
            "SREC_EligibleProduction",
            "InitialMeterValue",
            "MeterValue",
            "MeterDate",
            "DateTimeReported"
        ]

    def process_new_files(self):
        for file in os.listdir(DOWNLOAD_DIR):
            if file != 'desktop.ini':
                file_name = os.path.basename(file)
                self.process_file(os.path.join(DOWNLOAD_DIR, file_name))
        self._clear_existing_data()
        self.push_data_to_table()

    def process_file(self, file):
        file_name = os.path.basename(file).split(' ')
        quarter = file_name[2].replace('.csv', '')
        year = file_name[1]
        data = pd.read_csv(file, skiprows=[1, 2], header=1)
        if 'final' in os.path.basename(file).lower():
            columns_to_keep = [x.replace('1', '') for x in self.columns_to_keep]
        else:
            columns_to_keep = self.columns_to_keep
        data = data[columns_to_keep]
        data['Quarter'] = quarter + '-' + year
        data = data.values.tolist()
        self.data = self.data + data

    def _clear_existing_data(self):

        query = 'DELETE FROM D_POST_INSTALL.T_PTS_GEN pg ' \
                'WHERE pg.QUARTER_NAME IN (\'{a}\', \'{b}\', \'{c}\') ' \
                'OR pg.QUARTER_NAME IS NULL'.format(a=self.quarter_strings[0],
                                                    b=self.quarter_strings[1],
                                                    c=self.quarter_strings[2])
        self.db.open_connection()
        try:
            self.db.execute_sql_command(query)
        finally:
            self.db.close_connection()

    def push_data_to_table(self):
        if len(self.data) > 0:
            try:
                self.db.open_connection()
                self.db.insert_into_table('D_POST_INSTALL.T_PTS_GEN', self.data)
            finally:
                self.db.close_connection()


def clear_downloads():
    for file in os.listdir(DOWNLOAD_DIR):
        if file != 'desktop.ini':
            os.unlink(os.path.join(DOWNLOAD_DIR, file))


if __name__ == '__main__':
    LOGIN_URL = 'https://www.masscec-pts.com/login.aspx'
    REPORT_URL = 'https://www.masscec-pts.com/ReportingServicesNew.aspx'

    payload = {
        'username': {
            'html_name': 'txtUserNm',
            'value': os.environ.get('PTS_USERNAME')
        },
        'password': {
            'html_name': 'txtPwd',
            'value': os.environ.get('PTS_PASSWORD')
        },
        'button': {
            'id': 'ctl00_cphBody_ucLogin1_rbLogin_input'
        }
    }

    clear_downloads()

    crawler = PrimaryCrawler(payload, 'chrome')
    crawler.run_crawler()

    processor = FileProcessor(crawler.quarter_year_list)
    processor.process_new_files()
