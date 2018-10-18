"""
This automation will support sending NPS surveys to LeaseDimensions, which is one of Bryce's highest priorities for our team right now. Of course, feel free to work with Steven to prioritize.

---

If possible, could you please help get LD's notes to the Data Warehouse on a daily basis?

Locate files
ShareFile: Folders > Shared Folders > Collections > Daily Note Reports
Link: https://leasedimensions.sharefile.com/home/shared/fo7fe20f-4b2d-407f-952d-da3f9d153f54

Download files
Get anything new. Easiest would probably be to sort by "Uploaded" descending and grab anything that was uploaded yesterday (with daily automation). Note that some days have multiple files uploaded.

Process files
The files have some header rows that are pointless. The number of rows is not consistent.

I need these columns in the data warehouse:
user_name_s AS ld_user_name (NVARCHAR2(15 CHAR))
les_s AS solar_billing_account_number (NVARCHAR2(30 CHAR))
days_delq_l AS days_delinquent (NUMBER)
ref_amt_d AS amount_due (NUMBER)
d_ent_s AS created_date (DATE)
note_title_s AS notes (CLOB)
d_ent_s may need some special processing in Python or SQL to handle its format. I don't need the time component, if it's easier to do just the date.

We might need note_title_s to be filtered in the future; I will keep you posted.

Table details
Indexes on solar_billing_account_number and created_date.

"""

import os
import sys
import csv
import pandas as pd

from time import sleep
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from models import SnowFlakeDW, SnowflakeConsole

CHROME_DRIVER_PATH = r'C:\\Users\\jonathan.lauret\\Google Drive\\Projects\\Chrome Driver\chromedriver.exe'
FIREFOX_DRIVER_PATH = r'C:\\Users\\jonathan.lauret\\Google Drive\\Projects\\Chrome Driver\geckodriver.exe'


def find_main_dir():
    """
    Returns the Directory of the main running file
    """
    if getattr(sys, 'frozen', False):
        # The application is frozen
        return os.path.dirname(sys.executable)

    else:
        # The application is not frozen
        # Change this bit to match where you store your data files:
        return os.path.dirname(os.path.realpath(__file__))


class PrimaryCrawler:
    """
    Primary Crawler is a web crawler that will download 2 reports
    from Webstations and rename the files
    """
    delay = 10
    num_reports = 3
    crawler_log = {
        'login': False,
        'number_of_downloads': 0
    }

    def __init__(self, driver, testing=False):
        # Primary Items
        self.testing = testing
        self.skip = False
        self.driver = driver

        self.fp = webdriver.FirefoxProfile()

        if testing:
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
        if not self.testing:
            self.options.add_argument('headless')

    def login(self):
        # Use Driver to login
        # Go to Login Page
        print('Logging into {url}'.format(url=LOGIN_URL))
        self.DRIVER.get(LOGIN_URL)

        WebDriverWait(self.DRIVER, self.delay)\
            .until(EC.presence_of_element_located((By.ID, payload['username']['html_name'])))
        # Insert Username
        uname = self.DRIVER.find_element_by_id(payload['username']['html_name'])
        uname.send_keys(payload['username']['value'])

        # Insert Password
        passw = self.DRIVER.find_element_by_id(payload['password']['html_name'])
        passw.send_keys(payload['password']['value'])

        # Click the Login button
        login_button = payload['button']['class_name']
        self.DRIVER.find_element_by_id(login_button).click()
        self.crawler_log['login'] = True

    def locate_current_folder(self):
        print('Loading {url}'.format(url=REPORT_URL))
        self.DRIVER.get(REPORT_URL)
        try:
            WebDriverWait(self.DRIVER, self.delay)\
                .until(EC.presence_of_all_elements_located((By.CLASS_NAME, 'table_16ftul1')))
        except Exception as e:
            print(e)

        folders_table = self.DRIVER.find_element_by_class_name('table_16ftul1')
        folders_thead = folders_table.find_element(By.TAG_NAME, 'thead')
        folders_thead = folders_thead.find_element(By.TAG_NAME, 'tr')
        folders_thead = folders_thead.find_elements(By.TAG_NAME, 'th')
        header = [x.text.lstrip().rstrip() for x in folders_thead]
        folders_tbody = folders_table.find_element(By.TAG_NAME, 'tbody')
        rows = folders_tbody.find_elements(By.TAG_NAME, 'tr')

        new_table = []
        for j, row in enumerate(rows):
            new_row = []
            cols = row.find_elements(By.TAG_NAME, 'td')
            for i, col in enumerate(cols):
                if header.index('Name') == i:
                    col_name = col.text.lstrip().rstrip()
                    if col_name not in self.checked_folders:
                        self.checked_folders.append(col_name)
                        col.find_element(By.TAG_NAME, 'div').find_element(By.TAG_NAME, 'div').click()
                        self.check_page_for_downloads()
                        if len(rows) > len(self.checked_folders):
                            break
                        else:
                            self.checked_all_pages = True
            if self.iterations == j:
                break

    def check_page_for_downloads(self):
        try:
            # Wait for the Today table to load and define it
            print('Waiting for table to load')
            WebDriverWait(self.DRIVER, self.delay) \
                .until(EC.presence_of_element_located((By.CLASS_NAME, 'table_16ftul1')))
            reports_table = self.DRIVER.find_element_by_class_name('table_16ftul1')
        except Exception as e:
            print(e)

        new_table = []
        # Get the rows from the table
        print('Finding reports')
        rows = reports_table.find_elements(By.TAG_NAME, 'tr')

        # Iterate through rows, find links and text and store them
        for row in rows:
            new_row = []
            cols = row.find_elements(By.TAG_NAME, 'td')
            for col in cols:
                if 'VSLR' in col.text:
                    new_row.append(col.text.lstrip().rstrip())
            new_table.append(new_row)

        del new_table[0]
        new_table = [x[0] for x in new_table if x]

        for file_name in new_table:
            downloaded_files = read_download_file()
            if file_name not in downloaded_files:
                self.download_files(file_name)
                write_download_file(file_name)

    def run_crawler(self):
        # Login to page
        self.login()
        sleep(4)
        # Call URL
        if testing:
            print(self.DRIVER.current_url)

        # Go to Reports Page
        self.checked_all_pages = False
        self.checked_folders = []
        self.iterations = 0
        sleep(10)
        while not self.checked_all_pages:
            self.locate_current_folder()
            self.iterations += 1

        # Shutdown the Crawler
        self.end_crawl()

    def download_files(self, title):
        xpath = '//*[@title=\"{title}\"]'.format(title=title)
        # Click on name in table
        if self.testing:
            print(xpath)
        print('Attempting to download {title}'.format(title=title))
        found_item = self.DRIVER.find_element_by_xpath(xpath)
        found_item_parent = found_item.find_element_by_xpath('..')
        try:
            print('Clicking Item')
            found_item.click()
            # Wait for Download button to appear then click on it
            print('Waiting for Download Button')
            try:
                WebDriverWait(self.DRIVER, self.delay) \
                    .until(EC.presence_of_element_located((By.CLASS_NAME, 'buttonWrapper_18jq2rj')))
                self.DRIVER.find_element_by_class_name('buttonWrapper_18jq2rj').click()
            except: pass
            while not os.path.exists(os.path.join(DOWNLOAD_DIR, title)):
                sleep(.5)

            # Click on the back arrow
            self.DRIVER.find_element_by_class_name('backButton_1fhxzk6').click()
            sleep(2)
            # Wait for table to repopulate
            WebDriverWait(self.DRIVER, self.delay) \
                .until(EC.presence_of_element_located((By.CLASS_NAME, 'table_16ftul1')))
        except:
            print('Failed First Click')
            print('Clicking Parent')
            found_item_parent.click()
            print('Waiting for Download Button')
            WebDriverWait(self.DRIVER, self.delay) \
                .until(EC.presence_of_element_located((By.CLASS_NAME, 'button_1y18368')))
            print('Found Download Button')
            download_button = self.DRIVER.find_element_by_class_name('button_1y18368')
            print('Clicking Download Button')
            download_button.click()
            print('Waiting for Download to Complete')
            while not os.path.exists(os.path.join(DOWNLOAD_DIR, title)):
                sleep(.5)

    def end_crawl(self):
        # Close the Driver
        print('Crawler Tasks Complete')
        self.DRIVER.quit()


class CsvProcessor:

    def __init__(self, file_path):
        self.file_path = file_path
        self.data = []
        self.columns_to_keep = [
            'user_name_s',
            'les_s',
            'days_delq_l',
            'ref_amt_d',
            'd_ent_s',
            'note_title_s',
        ]

    def process_file(self):
        with open(self.file_path, 'r', encoding='utf8', errors='ignore') as f:
            reader = csv.reader(f, dialect='excel')
            for row in reader:
                self.data.append(row)
        self.remove_useless_rows()
        self.get_required_columns()
        self.filter_lines()
        self.add_upload_date()

    def remove_useless_rows(self):
        delete_row = 0
        for i, row in enumerate(self.data):
            if row[0] == 'web_acct_s':
                delete_row = i
                break
        del self.data[0:delete_row]

        self.data = [x for x in self.data if x[0] != '' and x[0] != ' ']

    def get_required_columns(self):
        columns = self.data[0]
        del self.data[0]
        self.data = pd.DataFrame(self.data, columns=columns)
        self.data['d_ent_s'] = pd.to_datetime(self.data['d_ent_s'], format='%b %d %Y %I:%M:%S:%f%p')

        for name in list(self.data.columns.values):
            if name not in self.columns_to_keep:
                del self.data[name]

        self.data = [list(self.data.columns.values)] + self.data.values.tolist()

    def filter_lines(self):
        header = self.data[0]
        del self.data[0]

        self.data = [row for row in self.data
                     if 'OC' in row[header.index('note_title_s')]
                     or 'IC' in row[header.index('note_title_s')]]

        for row in self.data:
            del row[header.index('note_title_s')]

        del header[header.index('note_title_s')]
        self.data = [header] + self.data

    def add_upload_date(self):

        header = self.data[0]
        del self.data[0]

        for row in self.data:
            row.append(datetime.today().date())

        header.append('upload_date')

        self.data = [header] + self.data


def write_download_file(string):
    download_file = open(DOWNLOAD_FILE_PATH, 'a')
    download_file.write(string + '\n')
    download_file.close()


def read_download_file():
    return open(DOWNLOAD_FILE_PATH, 'r').read().split('\n')


def write_process_file(string):
    processed_file = open(PROCESSED_FILE_PATH, 'a')
    processed_file.write(string + '\n')
    processed_file.close()


def read_process_file():
    return open(PROCESSED_FILE_PATH, 'r').read().split('\n')


if __name__ == '__main__':
    testing = True
    DOWNLOAD_DIR = os.path.join(find_main_dir(), 'downloads')

    DOWNLOAD_FILE_PATH = os.path.join(find_main_dir(), 'DownloadFile.txt')
    PROCESSED_FILE_PATH = os.path.join(find_main_dir(), 'ProcessFile.txt')

    LOGIN_URL = 'https://leasedimensions.sharefile.com/Authentication/Login'
    REPORT_URL = 'https://leasedimensions.sharefile.com/home/shared/fo7fe20f-4b2d-407f-952d-da3f9d153f54'

    payload = {
        'username': {
            'html_name': 'credentials-email',
            'value': os.environ.get('LD_USERNAME')
        },
        'password': {
            'html_name': 'credentials-password',
            'value': os.environ.get('LD_PASSWORD')
        },
        'button': {
            'class_name': 'start-button'
        }
    }

    TABLE_NAME = 'D_POST_INSTALL.T_LEASE_DIMENSION'

    pc = PrimaryCrawler('chrome', testing=testing)
    pc.run_crawler()

    db = SnowFlakeDW()
    db.set_user('JDLAURET')
    try:
        db.open_connection()
        dw = SnowflakeConsole(db)
        for file in os.listdir(DOWNLOAD_DIR):
            if '.csv' in file and file not in read_process_file():
                current_file = CsvProcessor(os.path.join(DOWNLOAD_DIR, file))
                current_file.process_file()
                dw.insert_into_table(TABLE_NAME, current_file.data, header_included=True)
                write_process_file(file)
    finally:
        db.close_connection()
