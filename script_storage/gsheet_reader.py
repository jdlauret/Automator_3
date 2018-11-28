import os
import sys
import pandas as pd
import time
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from BI.data_warehouse import Snowflake
from BI.google import GSheets

relations_table_name = 'D_POST_INSTALL.T_REL_NOTES'
fund_notes_table_name = 'D_POST_INSTALL.T_REL_FUND_NOTES'

west_1_workbook_key = '1M4pbep1-hAgmNz1oUMCS6r6gwO4EFKJi4m86RLmoOGk'
west_2_workbook_key = '1meZu0zgV6KQrs4tlIBoeuTaLWLYvwX4dbGnpU2BbQqs'
east_1_workbook_key = '1HsKTePHErUmraAW4go_XO-qvbzmg3Adue3jpF3myC3U'
east_2_workbook_key = '1oZ0KEYZCSHzm20oPgh4o5y6HLvvTQd8aGEiybZnEQ-0'
fund_notes_workbook_key = '1P67TLT-ClvCqeChbXiNyMrVz808IncmzPD3cbkmY6lc'

west_1_workbook_sheet_names = [
    'Team 02',
    'Team 03',
    'Team 06',
]

west_2_workbook_sheet_names = [
    'Team 01',
    'Team 05',
    'Team 04',
]

east_1_workbook_sheet_names = [
    'Team 07',
    'Team 08',
    'Team 09',
]

east_2_workbook_sheet_names = [
    'Team 10',
    'Team 11',
    'Team 12',
]

fund_notes_sheet_names = [
    'Fund 22',
    'Fund 23',
    'Fund 24',
    'Balance Sheet/Other',
]

workbooks = {
    'west_1_workbook': {
        'sheet_id': west_1_workbook_key,
        'sheets': west_1_workbook_sheet_names,
        'table': relations_table_name,
    },
    'east_1_workbook': {
        'sheet_id': east_1_workbook_key,
        'sheets': east_1_workbook_sheet_names,
        'table': relations_table_name,
    },
    'east_2_workbook': {
        'sheet_id': east_2_workbook_key,
        'sheets': east_2_workbook_sheet_names,
        'table': relations_table_name,
    },
    'fund_manager_workbook': {
        'sheet_id': fund_notes_workbook_key,
        'sheets': fund_notes_sheet_names,
        'table': fund_notes_table_name
    },
    'west_2_workbook': {
        'sheet_id': west_2_workbook_key,
        'sheets': west_2_workbook_sheet_names,
        'table': relations_table_name,
    },
}


class GsheetRetriever:
    def __init__(self, sheet_id, sheet_name):
        self.sheet_id = sheet_id
        self.sheet_name = sheet_name

        self.column_names = [
            'Project ID',
            'Team Notes',
            'Agent Ownership?',
            'Last Follow Up Date',
            'Scheduled Follow Up?'
        ]

        self.sheet_header = []
        self.sheet_data = []
        self.gs = GSheets(self.sheet_id)

    def retrieve_sheet_data(self):
        self.gs.set_active_sheet(self.sheet_name)
        self.gs.get_sheet_data()
        self.sheet_data = self.gs.results
        self.sheet_header = self.sheet_data[0]
        counter = 1
        for i, item in enumerate(self.sheet_header):
            if item.lower() == 'do not edit':
                self.sheet_header[i] = 'do not edit ' + str(counter)
                counter += 1
        del self.sheet_data[0]

    def shrink_data_set(self):
        self.sheet_data = pd.DataFrame(self.sheet_data, columns=self.sheet_header)

        for col_name in list(self.sheet_data.columns.values):
            if col_name not in self.column_names:
                self.sheet_data.drop(col_name, axis=1, inplace=True)

        self.sheet_data = self.sheet_data.values.tolist()


if __name__ == '__main__':
    db = Snowflake()
    db.set_user('JDLAURET')
    db.open_connection()
    tables_to_clear = []
    for key in workbooks.keys():
        if workbooks[key]['table'] not in tables_to_clear:
            tables_to_clear.append(workbooks[key]['table'])

    for table in tables_to_clear:
        db._clear_table(table)

    for key in workbooks.keys():
        current_sheet_id = workbooks[key]['sheet_id']
        table_name = workbooks[key]['table']
        for sheet_name in workbooks[key]['sheets']:
            # print('Getting Data from {}'.format(sheet_name))
            data_set = GsheetRetriever(current_sheet_id, sheet_name)
            data_set.retrieve_sheet_data()
            data_set.shrink_data_set()
            upload_set = data_set.sheet_data
            if sheet_name == 'Fund 23':
                db.insert_into_table(table_name, upload_set, header_included=False)
            else:
                db.insert_into_table(table_name, upload_set, header_included=False)
    db.close_connection()