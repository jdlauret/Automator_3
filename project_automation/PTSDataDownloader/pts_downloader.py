import datetime as dt
import json
import os
from time import sleep

import pandas as pd
from BI.data_warehouse import SnowflakeV2, SnowflakeConnectionHandlerV2
from BI.web_crawler import CrawlerBase
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait, Select

CHROME_DRIVER_PATH = r'C:\\Users\\jonathan.lauret\\Google Drive\\Projects\\Chrome Driver\chromedriver.exe'
FIREFOX_DRIVER_PATH = r'C:\\Users\\jonathan.lauret\\Google Drive\\Projects\\Chrome Driver\geckodriver.exe'

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
        if self.console_output:
            print('Loading {url}'.format(url=REPORT_URL))
        # Go to Report URL
        self.driver.get(REPORT_URL)

        # Read Through Dates in Downloads Json
        WebDriverWait(self.driver, self.delay).until(
            EC.presence_of_element_located((By.ID, 'ctl00_cphBody_btnIndividualSystems_CSV')))

        quarter_drop_down = 'ctl00$cphBody$ddlIndividualSystems_Quarter'
        year_drop_down = 'ctl00$cphBody$ddlIndividualSystems_Year'
        for quarter, year in self._create_quarter_year_list():
            quarter_string = 'Q' + str(quarter)
            year_string = str(year)
            self.driver.find_element_by_id('Skinnedctl00_cphBody_ddlIndividualSystems_Quarter').click()
            quarter_select = Select(self.driver.find_element_by_xpath("//select[@name=\"" + quarter_drop_down + "\"]"))
            quarter_select.select_by_value(str(quarter))
            year_select = Select(self.driver.find_element_by_xpath("//select[@name=\'" + year_drop_down + "\']"))
            year_select.select_by_value(str(year))

            # Click Download Button
            self.driver.find_element_by_id('ctl00_cphBody_btnIndividualSystems_CSV').click()
            file_name_1 = 'IndvSystemsProductionPreliminary ' + year_string + ' ' + quarter_string + '.csv'
            file_name_2 = 'IndvSystemsProductionFinal ' + year_string + ' ' + quarter_string + '.csv'
            while not os.path.isfile(os.path.join(DOWNLOAD_DIR, file_name_1)) \
                    and not os.path.isfile(os.path.join(DOWNLOAD_DIR, file_name_2)):
                if self.console_output:
                    print('sleeping .1')
                sleep(.1)

    def run_crawler(self):
        payload = {
            'url': 'https://www.masscec-pts.com/login.aspx',
            'username': {'input': os.environ.get('PTS_USERNAME'), 'id': 'txtUserNm', },
            'password': {'input': os.environ.get('PTS_PASS'), 'id': 'txtPwd', },
            'submit': {'id': 'ctl00_cphBody_ucLogin1_rbLogin_input', },
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

    def __init__(self, quarter_year_list):
        self.data = []
        self.db = SnowflakeV2(SnowflakeConnectionHandlerV2())
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
        self.db.open_connection()
        try:
            self._clear_existing_data()
            self.push_data_to_table()
        finally:
            self.db.close_connection()

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
        self.db.execute_sql_command(query)

    def push_data_to_table(self):
        if len(self.data) > 0:
            self.db.insert_into_table('D_POST_INSTALL.T_PTS_GEN', self.data)


def clear_downloads():
    for file in os.listdir(DOWNLOAD_DIR):
        if file != 'desktop.ini':
            os.unlink(os.path.join(DOWNLOAD_DIR, file))


if __name__ == '__main__':
    REPORT_URL = 'https://www.masscec-pts.com/ReportingServicesNew.aspx'

    clear_downloads()
    crawler = PrimaryCrawler('chrome', download_directory=DOWNLOAD_DIR)
    try:
        crawler.run_crawler()

        processor = FileProcessor(crawler.quarter_year_list)
        processor.process_new_files()
    finally:
        if crawler.active:
            crawler.end_crawl()
