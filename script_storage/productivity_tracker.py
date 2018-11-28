import datetime as dt

from BI.data_warehouse import Snowflake
from BI.google import GSheets


def add_date(list):
    timestamp = dt.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    for row in list:
        row.append(timestamp)
    return list


if __name__ == '__main__':
    db = Snowflake()
    db.set_user('JDLAURET')
    db.open_connection()
    dw_schema = 'D_POST_INSTALL'

    sheets_dict = {
        'srec_error_tracker': {
            'sheet_key': '1Z-4CnqFdflmtnxATvQXDG8qqgQ3Z1z8A7aLNaSqGZq8',
            'sheet_name': 'DWH',
            'table_name': dw_schema + '.T_SREC_ERROR_TRACKER',
            'clear_table': True,
            'add_upload_date': False,
            'header_included': False,
        },
        'activity_base_times': {
            'sheet_key': '1bexRA1MwDEKHNhOLN05yp0rPvVVOzYJwSP7NwY8zmXU',
            'sheet_name': 'DWH Input',
            'table_name': dw_schema + '.T_SREC_ACTIVITY_BASE_TIMES',
            'clear_table': True,
            'add_upload_date': True,
            'header_included': False,
        },
        'project_time': {
            'sheet_key': '1JtWr9NsCpFW8-rhY4ZYg4Nk0MLf5nkgMqBKLJNf1DU4',
            'sheet_name': 'DWH Input',
            'table_name': dw_schema + '.T_SREC_PROJECT_TIMES',
            'clear_table': True,
            'add_upload_date': True,
            'header_included': False,
        },
    }
    for sheet, sheet_values in sheets_dict.items():
        sheet_key = sheet_values.get('sheet_key')
        sheet_name = sheet_values.get('sheet_name')
        table_name = sheet_values.get('table_name')
        clear_table = sheet_values.get('clear_table')
        add_upload_date = sheet_values.get('add_upload_date')
        header = sheet_values.get('header_included')

        if sheet_key and sheet_name and table_name:
            gs = GSheets(sheet_key)
            gs.set_active_sheet(sheet_name)
            gs.get_sheet_data()
            sheet_data = gs.results
            if add_upload_date:
                sheet_data = add_date(sheet_data)
            try:
                dw.insert_into_table(table_name, sheet_data, header_included=header, overwrite=clear_table)
            except Exception as e:
                print(e)

    db.close_connection()
