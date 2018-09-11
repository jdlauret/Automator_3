import copy
import csv
import datetime as dt
import multiprocessing as mp
import os
import sys
from models import SnowFlakeDW, SnowflakeConsole

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from word_list_generator import list_generator
from download_qualtrics_data import *
from qualtrics_processor import *


def find_data_file(filename):
    if getattr(sys, 'frozen', False):
        # The application is frozen
        data_dir = os.path.dirname(sys.executable)
    else:
        # The application is not frozen
        # Change this bit to match where you store your data files:
        data_dir = os.path.dirname(__file__)

    return os.path.join(data_dir, filename)


def find_main_dir():
    if getattr(sys, 'frozen', False):
        # The application is frozen
        return os.path.dirname(sys.executable)
    else:
        # The application is not frozen
        # Change this bit to match where you store your data files:
        return os.path.dirname(__file__)


def clear_temp_data():
    """
    Deletes all files contained in temp_files
    """
    print('Clearing Temp Data')
    temp_files = [f for f in os.listdir('temp_data')]
    for f in temp_files:
        os.remove(os.path.join('temp_data', f))


def clear_qualtrics_data():
    """
    Deletes all files contained in temp_files
    """
    print('Clearing Temp Data')
    temp_files = [f for f in os.listdir('qualtrics_data')]
    for f in temp_files:
        try:
            os.remove(os.path.join('qualtrics_data', f))
        except:
            pass


class CreateQualtricsJson:

    def __init__(self, file_name):
        self.file_name = file_name

    def create_lists_from_excel(self):
        """
        Opens each CSV file in the temp_data directory
        Then stores the list in a json
        :return:
        """
        fn = 'qualtrics_data\\' + self.file_name
        with open(fn, 'r', encoding='utf8') as f:
            new_list = list(list(rec) for rec in csv.reader((
                line.replace('\0', '') for line in f), delimiter=','))
        new_file_name = self.file_name.replace('csv', 'json')
        fn = os.path.join(find_main_dir(), 'data\\qualtrics_jsons\\' + new_file_name)
        try:
            file = open(fn, 'r')
        except IOError:
            file = open(fn, 'w')
        file.close()
        file = open(fn, 'w')
        new_obj = {str(new_file_name.replace('.json', '')): new_list}
        json.dump(new_obj, file, sort_keys=True, indent=4)
        file.close()


def get_file(file_name, folder_name):
    """
    Creates file string
    :param file_name:
    :param folder_name:
    :return: returns concatenated string
    """
    return os.path.join(find_main_dir(), folder_name + '/' + file_name)


def get_list(key):
    """
    Returns a list from word_and_id json using key
    :param key:
    :return:
    """
    with open(get_file('nps_word_pairs.json', 'data')) as infile:
        word_and_id = json.load(infile)

    word_list = [
        [
            'Survey ID',
            key,
            'NPS Type'
        ]
    ]

    for survey_id in word_and_id.keys():

        for word in word_and_id[survey_id][key]['NPS']['Word_List']:
            nps_row = [survey_id, word, 'NPS']
            word_list.append(nps_row)
        for word in word_and_id[survey_id][key]['ANPS']['Word_List']:
            anps_row = [survey_id, word, 'ANPS']
            word_list.append(anps_row)

    return word_list


def print_progress(iteration, total, prefix='', suffix='', decimals=1, bar_length=100):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        bar_length  - Optional  : character length of bar (Int)
    """
    str_format = "{0:." + str(decimals) + "f}"
    percents = str_format.format(100 * (iteration / float(total)))
    filled_length = int(round(bar_length * iteration / float(total)))
    bar = '█' * filled_length + '-' * (bar_length - filled_length)

    sys.stdout.write('\r%s |%s| %s%s %s' % (prefix, bar, percents, '%', suffix)),

    if iteration == total:
        sys.stdout.write('\n')
    sys.stdout.flush()


def combine_list(list_1, list_2):
    """
    Combine 2 list of lists together removing the header from the second list
    :param list_1:
    :param list_2:
    :return:
    """
    del list_2[0]
    return list_1 + list_2


survey_dict = {
    'nps_intro_call': {
        'name': 'NPS Intro Call',
        'sid': "SV_0ewdZ9Dh6U5ouax",
        'api_token': os.environ.get('QUALTRICS_MAIN_API_TOKEN'),
        'data_center': 'az1',
    },
    'nps_at_fault_home_damage': {
        'name': 'NPS At-Fault Home Damage',
        'sid': "SV_0032fulVyVV5rEN",
        'api_token': os.environ.get('QUALTRICS_MAIN_API_TOKEN'),
        'data_center': 'az1',
    },
    'nps_home_upgrade': {
        'sid': "SV_2tv9qp2qegueuln",
        'name': 'NPS Home Upgrade',
        'api_token': os.environ.get('QUALTRICS_MAIN_API_TOKEN'),
        'data_center': 'az1',
    },
    'nps_customer_service': {
        'sid': "SV_0cQts5G0eOs5pOZ",
        'name': 'NPS Customer Service',
        'api_token': os.environ.get('QUALTRICS_MAIN_API_TOKEN'),
        'data_center': 'az1',
    },
    'nps_executive_resolutions_case_closed': {
        'sid': "SV_71K480bakSFDmQd",
        'name': 'NPS Executive Resolutions Case Close',
        'api_token': os.environ.get('QUALTRICS_MAIN_API_TOKEN'),
        'data_center': 'az1',
    },
    'nps_customer_success_managers': {
        'sid': "SV_6WlagN6igyFaO7r",
        'name': 'NPS Customer Success Managers',
        'api_token': os.environ.get('QUALTRICS_MAIN_API_TOKEN'),
        'data_center': 'az1',
    },
    'nps_fin_complete': {
        'sid': "SV_23rGU3M7mVXWfn7",
        'name': 'NPS FIN Complete',
        'api_token': os.environ.get('QUALTRICS_MAIN_API_TOKEN'),
        'data_center': 'az1',
    },
    'nps_relational': {
        'sid': "SV_bNp2Xm2rZVtWab3",
        'name': 'NPS Relational',
        'api_token': os.environ.get('QUALTRICS_MAIN_API_TOKEN'),
        'data_center': 'az1',
    },
    'nps_usage_completed': {
        'sid': "SV_ctHi4PCGC2ZraS1",
        'name': 'NPS Usage Completed',
        'api_token': os.environ.get('QUALTRICS_MAIN_API_TOKEN'),
        'data_center': 'az1',
    },
    'nps_post_install_process': {
        'sid': "SV_eFnvsJjhOOPVowJ",
        'name': 'NPS Post Install Process',
        'api_token': os.environ.get('QUALTRICS_MAIN_API_TOKEN'),
        'data_center': 'az1',
    },
    'nps_pto': {
        'sid': "SV_29sKA97g0oEIlkF",
        'name': 'NPS PTO',
        'api_token': os.environ.get('QUALTRICS_MAIN_API_TOKEN'),
        'data_center': 'az1',
    },
    'nps_install_complete': {
        'sid': "SV_71XYFqoWgqrp8Md",
        'name': 'NPS Install Complete',
        'api_token': os.environ.get('QUALTRICS_MAIN_API_TOKEN'),
        'data_center': 'az1',
    },
    'nps_pto_45': {
        'sid': "SV_e5wEuDcHC9IIKHj",
        'name': 'NPS PTO Billing',
        'api_token': os.environ.get('QUALTRICS_MAIN_API_TOKEN'),
        'data_center': 'az1',
    },
    'nps_pre_install_process': {
        'sid': "SV_3yNCgLrPE6LV3mJ",
        'name': 'NPS Pre-Install-Process',
        'api_token': os.environ.get('QUALTRICS_MAIN_API_TOKEN'),
        'data_center': 'az1',
    },
    'nps_site_survey_complete': {
        'sid': "SV_8HyPxkVN38jXTkF",
        'name': 'NPS Site Survey Complete',
        'api_token': os.environ.get('QUALTRICS_MAIN_API_TOKEN'),
        'data_center': 'az1',
    },
    'post_pto_work_order': {
        'sid': 'SV_1Y4igxkM532zQod',
        'name': 'Post PTO Work Order',
        'api_token': os.environ.get('QUALTRICS_MAIN_API_TOKEN'),
        'data_center': 'az1',
    },
    'rts-om__feedback_loop': {
        'name': 'RTS - Field Feedback',
        'sid': "SV_8eUFJC9DEyzqdQF",
        'api_token': os.environ.get('QUALTRICS_HR_API_TOKEN'),
        'data_center': 'az1',
    },
    'LD': {
        'name': 'LD',
        'sid': 'SV_4YKHh79nXj5PuUl',
        'api_token': os.environ.get('QUALTRICS_MAIN_API_TOKEN'),
        'data_center': 'az1',
    }
}


def worker(arg):
    obj, meth_name = arg[:2]
    return getattr(obj, meth_name)(*arg[2:])


if __name__ == '__main__':

    db = SnowFlakeDW()
    db.set_user('JDLAURET')
    db.open_connection()
    dw = SnowflakeConsole(db)
    testing = True

    get_new_data = True
    create_workbooks = False
    update_tables = True
    words_reset = False

    NUM_CORE = 10
    MAIN_DIR = find_main_dir()
    TEMP_FOLDER = os.path.join(MAIN_DIR, 'temp_data')
    DATA_FOLDER = os.path.join(MAIN_DIR, 'data')
    INPUT_DIR = os.path.join(MAIN_DIR, 'input')

    if testing:
        print(MAIN_DIR)

    # Dates
    start = dt.datetime.now()
    today = dt.datetime.now()
    today = str(today)

    if get_new_data:
        pool1 = mp.Pool(NUM_CORE)
        try:
            clear_qualtrics_data()
        except:
            pass
        print('Downloading Qualtrics Data')
        survey_objects = [DownloadSurveyType(value) for key, value in survey_dict.items()]
        pool1.map(worker, ((obj, 'get_qualtrics_data') for obj in survey_objects))

        files_for_extraction = [filename for filename in os.listdir('qualtrics_data') if filename.endswith('.csv')]
        extraction_objects = [CreateQualtricsJson(file_name) for file_name in files_for_extraction]

        print('Extracting Qualtrics Survey Data')
        pool1.map(worker, ((obj, 'create_lists_from_excel') for obj in extraction_objects))
        print('Survey Data Processed and Saved')
        pool1.close()
        pool1.join()

    PUBLIC_NPS_TABLE = 'D_POST_INSTALL.T_NPS_SURVEY_RESPONSE'
    NPS_FILE_ID = '0B9Fc6ijLP56VbmFGc0V5MzZOU00'

    print('Reformatting Qualtrics Data')
    data_warehouse_data = qualtrics_processor()
    public_data_warehouse_data = format_survey_data(data_warehouse_data.get('public'))
    private_data_warehouse_data = copy.deepcopy(public_data_warehouse_data)
    print('Qualtrics formatted and in memory')

    # NPS Dash - Projects v2.sql

    word_cloud_table = 'D_POST_INSTALL.T_NPS_WORD_CLOUD_DATA'
    print('Download Complete')

    reviewed_data_file = 'reviewed_surveys'
    data_dir = os.getcwd() + '\\data'
    workbook_name = 'NPS Survey Data.xlsx'

    # Reset will clear relevant JSON files when a reset of the data is required.
    word_cloud_data = get_list(list_generator(public_data_warehouse_data,
                                              public_data_warehouse_data[0],
                                              1,
                                              reset=words_reset))

    if update_tables:
        dw.insert_into_table(word_cloud_table, word_cloud_data, header_included=True, overwrite=True)

        dw.insert_into_table(PUBLIC_NPS_TABLE, public_data_warehouse_data, header_included=True, overwrite=True)

    end = dt.datetime.now()
    duration = end - start
    print('All Tasks Completed in {0} seconds'.format(duration.total_seconds()))
    db.close_connection()
