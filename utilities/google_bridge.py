# standard library imports
import os
import sys
# related third party imports
import httplib2
# local application/library specific imports
from time import sleep
from googleapiclient import discovery
from googleapiclient.errors import HttpError
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import gspread
import pygsheets
from itertools import chain


def find_data_file(filename):
    if getattr(sys, 'frozen', False):
        # The application is frozen
        data_dir = os.path.dirname(sys.executable)
    else:
        # The application is not frozen
        # Change this bit to match where you store your data files:
        data_dir = os.path.dirname(__file__)

    return os.path.join(data_dir, filename)


def find_file(filename):
    if getattr(sys, 'frozen', False):
        # The application is frozen
        data_dir = os.path.dirname(sys.executable)
    else:
        # The application is not frozen
        # Change this bit to match where you store your data files:
        data_dir = os.path.dirname(__file__)

    return os.path.join(find_main_dir(), filename)


def find_main_dir():
    if getattr(sys, 'frozen', False):
        # The application is frozen
        return os.path.dirname(sys.executable)
    else:
        # The application is not frozen
        # Change this bit to match where you store your data files:
        return os.path.dirname(__file__)


def split_list(alist, wanted_parts=1):
    length = len(alist)
    return [
        alist[i * length // wanted_parts: (i + 1) * length // wanted_parts]
        for i in range(wanted_parts)
    ]


SCOPES = 'https://www.googleapis.com/auth/drive'
CLIENT_SECRET_FILE = 'client_secrets.json'
APPLICATION_NAME = 'PythonDriveReader'
MAIN_DIR = find_main_dir()
gAuth = GoogleAuth()


def g_auth():
    gAuth.LoadCredentialsFile(find_data_file('mycreds.txt'))
    if gAuth.credentials is None:
        # Authenticate if they're not there
        gAuth.LocalWebserverAuth()
    elif gAuth.access_token_expired:
        # Refresh them if expired
        gAuth.Refresh()
    else:
        # Initialize the saved creds
        gAuth.Authorize()
    # Save the current credentials to a file
    gAuth.SaveCredentialsFile(find_data_file('mycreds.txt'))

    return GoogleDrive(gAuth)

try:
    import argparse

    flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
except ImportError:
    flags = None


def key_extractor(url):
    drive_key_len = 28
    spreadsheet_key_len = 44
    if url is not None:
        if len(url) > drive_key_len \
                or len(url) > spreadsheet_key_len:

            if 'file/d/' in url:
                file_start = url.index('file/d/') + len('file/d/')
                new_url = url[file_start:]
                return new_url[:new_url.index('/')]

            if 'spreadsheets/d/' in url:
                file_start = url.index('spreadsheets/d/') + len('spreadsheets/d/')
                new_url = url[file_start:]
                return new_url[:new_url.index('/')]

            if 'folders/' in url:
                file_start = url.index('folders/') + len('folders/')
                return url[file_start:]

            if 'id=' in url:
                file_start = url.index('id=') + len('id=')
                return url[file_start:]

    return url


def colnum_string(n):
    div = n
    string = ""
    while div > 0:
        mod = (div - 1) % 26
        string = chr(65 + mod) + string
        div = int((div - mod) / 26)
    return string


def client_secrets():
    return find_data_file(CLIENT_SECRET_FILE)


def get_credentials(location=False):
    home_dir = os.path.expanduser('~')
    credential_dir = os.path.join(home_dir, '.credentials')
    if not os.path.exists(credential_dir):
        os.makedirs(credential_dir)
    credential_path = os.path.join(credential_dir, 'client_secrets.json')

    if location is False:

        store = Storage(credential_path)
        credentials = store.get()
        if not credentials or credentials.invalid:
            flow = client.flow_from_clientsecrets(client_secrets(), SCOPES)
            flow.user_agent = APPLICATION_NAME
            if flags:
                credentials = tools.run_flow(flow, store, flags)
            print('Storing credentials to ' + credential_path)

        return credentials
    else:
        return credential_path


credentials = get_credentials()
http = credentials.authorize(httplib2.Http())
sheetApp = discovery.build('sheets', 'v4', http=http)
driveApp = discovery.build('drive', 'v3', http=http)
scriptApp = discovery.build('script', 'v1', http=http)


class GoogleSheetsAccess:
    ss_id = 'No ID'

    def __init__(self, spreadsheet_id):
        self.ss_id = spreadsheet_id

    def sheet_access_check(self):
        try:
            sheetApp.spreadsheet().get(spreadsheetId=self.ss_id, includeGridData=False).execute()
        except HttpError as err:
            if err.resp.status in [403, 500, 503]:
                sleep(5)
            else:
                raise


def get_google_sheet_data(ss_id, range_name):
    results = sheetApp.spreadsheets().values().get(spreadsheetId=ss_id,
                                                   range=range_name).execute()
    return results['values']


class GoogleSheets:
    ss_id = "No ID"
    sheet_name = "No Sheet Name"
    sheet_info = "No Sheet Data"
    sheet_id = "No Sheet Id"
    data = "No Data"
    clear = False
    gray = False

    def __init__(self, spreadsheet_id, sheet_name, data, clear=False, gray=False):
        self.ss_id = spreadsheet_id
        self.sheet_name = sheet_name
        self.sheet_info = self.sheet_data()
        self.sheet_id = self.sheet_info["sheetId"]
        self.data = data
        self.clear = clear
        self.gray = gray
        self.append()

    def get_sheets(self):

        try:
            results = sheetApp.spreadsheets().get(spreadsheetId=self.ss_id,
                                                  includeGridData=True).execute()
            return results["sheets"]
        except HttpError as err:
            if err.resp.status in [403, 500, 503]:
                sleep(5)
            else:
                raise

    def sheet_data(self):
        temp_sheet_name = self.sheet_name.lower()
        for sheet in self.get_sheets():
            if sheet["properties"]["title"].lower() == temp_sheet_name:
                return sheet["properties"]

    def clear_sheet(self):

        basics = self.sheet_info
        col_count = basics["gridProperties"]["columnCount"]

        range_name = ("{0}!A:" + str(colnum_string(col_count))).format(self.sheet_name)

        try:
            sheetApp.spreadsheets().values().clear(spreadsheetId=self.ss_id,
                                                   range=range_name,
                                                   body={}).execute()
        except HttpError as err:
            if err.resp.status in [403, 500, 503]:
                sleep(5)
            else:
                raise

    def resize_sheet(self):

        data_len = len(self.data)
        data_wid = len(self.data[0])
        sheet_cols = self.sheet_info['gridProperties']["columnCount"]
        sheet_rows = self.sheet_info["gridProperties"]["rowCount"]

        col_del_flag = False
        row_del_flag = False
        col_add_flag = False
        row_add_flag = False
        if self.clear:
            if sheet_cols > data_wid or sheet_rows > data_len:

                if sheet_cols > data_wid:
                    del_col_start = data_wid - 1
                    del_col_end = sheet_cols - 1
                    col_del_flag = True
                    remove_cols = {
                        "deleteDimension": {
                            "range": {
                                "sheetId": self.sheet_id,
                                "dimension": "COLUMNS",
                                "startIndex": del_col_start,
                                "endIndex": del_col_end
                            }
                        }
                    }

                if sheet_rows > data_len:
                    del_row_start = data_len - 1
                    del_row_end = sheet_rows - 1
                    row_del_flag = True
                    remove_rows = {
                        "deleteDimension": {
                            "range": {
                                "sheetId": self.sheet_id,
                                "dimension": "ROWS",
                                "startIndex": del_row_start,
                                "endIndex": del_row_end
                            }
                        }
                    }

        if sheet_cols < data_wid or sheet_rows < data_len:

            if sheet_cols < data_wid:
                add_cols_num = data_wid - sheet_cols
                col_add_flag = True
                add_cols = {
                    "appendDimension": {
                        "sheetId": self.sheet_id,
                        "dimension": "COLUMNS",
                        "length": add_cols_num
                    }
                }

            if sheet_rows < data_len:
                add_rows_num = data_len - sheet_rows
                row_add_flag = True
                add_rows = {
                    "appendDimension": {
                        "sheetId": self.sheet_id,
                        "dimension": "ROWS",
                        "length": add_rows_num
                    }
                }

        results = {
            "check": False,
            "data": []
        }

        if col_add_flag:
            results["data"].append(add_cols)
            results["check"] = True
        if row_add_flag:
            results["data"].append(add_rows)
            results["check"] = True
        if row_del_flag:
            results["data"].append(remove_rows)
            results["check"] = True
        if col_del_flag:
            results["data"].append(remove_cols)
            results["check"] = True

        return results

    def append(self):

        if self.clear:
            self.clear_sheet()

        value = lambda x: {  # noqa
            "userEnteredValue": {"stringValue": str(x)},
            "userEnteredFormat": {"backgroundColor": {
                "red": 0.8, "green": 0.8, "blue": 0.8, "alpha": 0.5}
            }
        } if self.gray else {
            "userEnteredValue": {"stringValue": str(x)}
        }

        rows = [{"values": [value(cell) for cell in row]} for row in self.data]

        body = {
            "requests": []
        }

        resized = self.resize_sheet()
        if resized["check"]:
            for obj in resized["data"]:
                body["requests"].append(obj)

        values = {
            "appendCells": {
                "sheetId": self.sheet_id,
                "rows": rows,
                "fields": "*",
            }
        }

        body["requests"].append(values)

        try:
            sheetApp.spreadsheets().batchUpdate(spreadsheetId=self.ss_id,
                                                body=body).execute()
        except HttpError as err:
            if err.resp.status in [403, 500, 503]:
                sleep(5)
            else:
                raise


class GoogleDriveAccess:
    ss_id = 'No ID'

    def __init__(self, folder_id):
        self.ss_id = folder_id


def sheet_access_check():
    try:
        GoogleDrive(gAuth)
    except HttpError as err:
        if err.resp.status in [403, 500, 503]:
            sleep(5)
        else:
            raise


def get_file_name(file_id):
    drive = g_auth()
    return str(drive.CreateFile({'id': str(file_id)})['title'])


def read_drive_file(file_id):
    drive = g_auth()
    f = drive.CreateFile({'id': file_id})
    return f.GetContentString()


def download_file(file_id, file_name):
    drive = g_auth()
    f = drive.CreateFile({'id': file_id})
    mime_type = f['mimeType']
    f.GetContentFile(file_name, mimetype=mime_type)


class GoogleDriveUploader:

    def __init__(self, file_name, file_path, folder_id, file_id=0):
        self.drive = g_auth()
        self.file_name = file_name
        self.file_path = file_path
        self.file_id = file_id
        if folder_id != '':
            self.folder_id = folder_id
        else:
            self.folder_id = 'root'

        self.file_list = self.list_files(self.folder_id)

        if self.file_id == 0:
            for item in self.file_list:
                if item['title'] == file_name:
                    self.file_id = item.get('id')
        self.file = os.path.join(file_path, file_name)

    def list_files(self, folder_id):
        return self.drive.ListFile(
            {
                'q': "'{folder_id}' in parents and trashed=false".format(folder_id=folder_id)
            }).GetList()

    def start_upload(self):
        if self.file_id == 0:
            f = self.drive.CreateFile(metadata=
                {
                    'title': self.file_name,
                    "parents": [{
                        "kind": "drive#fileLink",
                        "id": self.folder_id
                    }]
                })
        else:
            f = self.drive.CreateFile(metadata=
                {
                    'id': self.file_id,
                    'title': self.file_name,
                    "parents": [{
                        "kind": "drive#fileLink",
                        "id": self.folder_id}]
                })

        f.SetContentFile(self.file)
        f.Upload()


def get_sheet_data(ss_id, sheet_name, col_num=0, header_row=1):
    if col_num == 0:
        range_name = ("'{0}'!{1}:{1}".format(sheet_name, header_row))
        first_row = sheetApp.spreadsheets().values().get(spreadsheetId=ss_id,
                                                         range=range_name).execute()

        col_number = len(first_row.get('values')[0])

    else:
        col_number = col_num
    range_name = ("'{0}'!A:" + str(colnum_string(col_number))).format(sheet_name)
    results = sheetApp.spreadsheets().values().get(spreadsheetId=ss_id,
                                                   range=range_name).execute()

    return results.get('values')


def gsheet_write(ss_id, sheet_name, range_name, data, header_included=True):
    gc = gspread.authorize(credentials)
    spreadsheet = gc.open_by_key(ss_id)
    sheet = spreadsheet.worksheet(sheet_name)
    existing_rows = sheet.row_count
    existing_cols = sheet.col_count
    new_data = data.copy()
    max_wid = max(len(p) for p in new_data)
    max_len = len(new_data)

    if header_included:
        del new_data[0]

    if existing_rows < max_len:
        sheet.add_rows(max_len - existing_rows)
        
    if existing_cols < max_wid:
        sheet.add_cols(max_wid - existing_cols)

    if len(new_data) > 0 and max_len < 5000:
        if isinstance(range_name, list):
            start_row = range_name[0]
            start_col = colnum_string(range_name[1])
            end_row = len(new_data) + (range_name[1])
            end_col = colnum_string(max_wid + (range_name[0] - 2))
            alt_range_name = str(start_col) + str(start_row) + ':' + str(end_col) + str(end_row)
            cell_list = sheet.range(alt_range_name)
        else:
            cell_list = sheet.range(range_name)
        if isinstance(new_data[0], tuple):
            for i, row in enumerate(new_data):
                new_data[i] = list(row)

        if isinstance(new_data[0], list):
            flat_list = list(chain.from_iterable(new_data))
            for i, item in enumerate(flat_list):
                if item is None:
                    flat_list[i] = ''

            for i, cell in enumerate(cell_list):
                cell.value = flat_list[i]
            sheet.update_cells(cell_list)
        else:
            for i, cell in enumerate(cell_list):
                cell.value = new_data[i]
            sheet.update_cells(cell_list)
    elif len(new_data) > 5000:
        divided_list = split_list(new_data, wanted_parts=round(len(new_data)/5000))
        list_lengths = 0
        for i, data_list in enumerate(divided_list):
            max_wid = max(len(p) for p in data_list)
            start_row = list_lengths + range_name[0]
            start_col = colnum_string(range_name[1])
            end_row = len(data_list) + list_lengths + (range_name[0] - 1)
            end_col = colnum_string(max_wid + (range_name[0] - 2))
            alt_range_name = str(start_col) + str(start_row) + ':' + str(end_col) + str(end_row)
            cell_list = sheet.range(alt_range_name)
            if isinstance(data_list, list):
                flat_list = list(chain.from_iterable(data_list))
                for i, item in enumerate(flat_list):
                    if item is None:
                        flat_list[i] = ''

                for i, cell in enumerate(cell_list):
                    cell.value = flat_list[i]
                sheet.update_cells(cell_list)
            list_lengths += len(data_list)


def gsheet_clear(ss_id, sheet_name, range_name, data):
    print('Clearing sheet {0}'.format(sheet_name))
    gc = gspread.authorize(credentials)
    spreadsheet = gc.open_by_key(ss_id)
    sheet = spreadsheet.worksheet(sheet_name)
    new_data = data.copy()
    max_len = len(new_data)
    max_wid = max(len(p) for p in new_data)

    if len(new_data) > 0 and max_len < 5000:
        if isinstance(range_name, list):
            start_row = range_name[0]
            start_col = colnum_string(range_name[1])
            end_row = len(new_data) + (range_name[1])
            end_col = colnum_string(max_wid + (range_name[0] - 2))
            alt_range_name = str(start_col) + str(start_row) + ':' + str(end_col) + str(end_row)
            cell_list = sheet.range(alt_range_name)
        else:
            cell_list = sheet.range(range_name)
        if isinstance(new_data[0], tuple):
            for i, row in enumerate(new_data):
                new_data[i] = list(row)

        if isinstance(new_data[0], list):
            flat_list = list(chain.from_iterable(new_data))
            for i, item in enumerate(flat_list):
                flat_list[i] = ''

            for i, cell in enumerate(cell_list):
                cell.value = ''
            sheet.update_cells(cell_list)
        else:
            for i, cell in enumerate(cell_list):
                cell.value = ''
            sheet.update_cells(cell_list)
    elif len(new_data) > 5000:
        divided_list = split_list(new_data, wanted_parts=round(len(new_data)/5000))
        list_lengths = 0
        for i, data_list in enumerate(divided_list):
            start_row = list_lengths + range_name[0]
            start_col = colnum_string(range_name[1])
            end_row = len(data_list) + list_lengths + (range_name[0] - 1)
            if int(end_row) > sheet.row_count:
                end_row = sheet.row_count
            end_col = colnum_string(max_wid + (range_name[0] - 2))
            alt_range_name = str(start_col) + str(start_row) + ':' + str(end_col) + str(end_row)
            cell_list = sheet.range(alt_range_name)

            if isinstance(data_list, list):
                flat_list = list(chain.from_iterable(data_list))
                for i, item in enumerate(flat_list):
                    flat_list[i] = ''

                for i, cell in enumerate(cell_list):
                    cell.value = ''
                sheet.update_cells(cell_list)
            list_lengths += len(data_list)

            if i == 0:
                # update cell params row, col, val
                sheet.update_cell(range_name[0], range_name[1], 'Update In Progress')

def range_builder(start_row, start_col, end_row=None, end_col=None):
    start_col_letter = colnum_string(start_col)

    if end_row is not None and end_col is not None:
        end_col_letter = colnum_string(end_col)
        range_name = str(start_col_letter) + str(start_row) + ':' + str(end_col_letter) + str(end_row)
    elif end_row is None and end_col is not None:
        end_col_letter = colnum_string(end_col)
        range_name = str(start_col_letter) + str(start_row) + ':' + str(end_col_letter)
    else:
        range_name = str(start_col_letter) + str(start_row)
    return range_name


def sheet_rows(sheet_id, sheet_name):
    gc = pygsheets.authorize(find_file(CLIENT_SECRET_FILE), no_cache=True)
    sh = gc.open_by_key(sheet_id)
    wks = sh.worksheet('title', sheet_name)
    return wks.rows


def add_rows(sheet_id, sheet_name, rows_to_insert):
    gc = pygsheets.authorize(find_file(CLIENT_SECRET_FILE), no_cache=True)
    sh = gc.open_by_key(sheet_id)
    wks = sh.worksheet('title', sheet_name)
    wks.add_rows(rows_to_insert)


def add_cols(sheet_id, sheet_name, cols_to_insert):
    gc = pygsheets.authorize(find_file(CLIENT_SECRET_FILE), no_cache=True)
    sh = gc.open_by_key(sheet_id)
    wks = sh.worksheet('title', sheet_name)
    wks.add_cols(cols_to_insert)


def sheet_cols(sheet_id, sheet_name):
    gc = pygsheets.authorize(find_file(CLIENT_SECRET_FILE), no_cache=True)
    sh = gc.open_by_key(sheet_id)
    wks = sh.worksheet('title', sheet_name)
    return wks.cols


def sheet_data(sheet_id, sheet_name):
    gc = pygsheets.authorize(find_file(CLIENT_SECRET_FILE), no_cache=True)
    sh = gc.open_by_key(sheet_id)
    wks = sh.worksheet('title', sheet_name)
    return wks.get_all_values()


def clear_sheet(sheet_id, sheet_name, start, end):
    gc = pygsheets.authorize(find_file(CLIENT_SECRET_FILE), no_cache=True)
    sh = gc.open_by_key(sheet_id)
    wks = sh.worksheet('title', sheet_name)
    wks.clear(start=start, end=end)


def update_sheet(sheet_id, sheet_name, start, data_frame):
    import pandas as pd
    import numpy as np
    gc = pygsheets.authorize(find_file(CLIENT_SECRET_FILE), no_cache=True, retries=10)
    sh = gc.open_by_key(sheet_id)
    wks = sh.worksheet('title', sheet_name)
    if not isinstance(data_frame, pd.DataFrame):
        df = pd.DataFrame(data_frame)
    else:
        df = data_frame
    df = df.replace(np.nan, '', regex=True)
    wks.set_dataframe(df, start, copy_head=False)


def update_named_range(sheet_id, sheet_name, range_name, value):
    gc = pygsheets.authorize(find_file(CLIENT_SECRET_FILE), no_cache=True)
    sh = gc.open_by_key(sheet_id)
    wks = sh.worksheet('title', sheet_name).update_cell(range_name, value)


def get_named_range(sheet_id, sheet_name, range_name):
    gc = pygsheets.authorize(find_file(CLIENT_SECRET_FILE), no_cache=True)
    sh = gc.open_by_key(sheet_id)
    wks = sh.worksheet('title', sheet_name)
    range = wks.get_named_range(range_name)
    return range
