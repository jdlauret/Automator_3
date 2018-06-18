from models import *

test_line = ['TEST_TASK', 'JD', 5, 'Daily', 'Daily', dt.datetime(2018, 1, 12, 0, 0, 0),
             dt.datetime(2018, 4, 27, 1, 54, 26), 'SQL', '0B9Fc6ijLP56VVWlqaTUwYlFYTnM', 'CSV', 'Google Drive',
             '0B9Fc6ijLP56VVTBOOWVVaWhNZUU', 'Data', 'Jonathan.lauret@vivintsolar.com', 'Non-Operational',
             'BA/PMO', 'FALSE', ]

test_column_map = ['NAMEX', 'OWNERX', 'MANUAL_TIME', 'MANUAL_RECURRENCE', 'AUTO_RECURRENCE', 'CREATED_DATE', 'LAST_RUN',
                   'DATA_SOURCE', 'DATA_SOURCE_ID', 'DATA_STORAGE_TYPE', 'STORAGE_TYPE', 'STORAGE_ID', 'FILE_NAME',
                   'OWNER_EMAIL', 'OPERATIONAL', 'DEPARTMENT', 'RUN_REQUESTED']

test_script = os.path.join(script_dir, 'ic_cdr.py')