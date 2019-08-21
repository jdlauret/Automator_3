import decimal

from . import *


class TaskOutput:

    def __init__(self, task):
        self.task = task
        self.dw = self.task.dw
        self.luna_dw = Postgres()
        self.luna_dw.login(os.environ.get('POSTGRES_HOST'))
        self.append = self.task.append
        self.SheetRange = self.task.SheetRange
        self.dynamic_name = self.task.dynamic_name
        self.output_type = self.task.data_storage_type
        self.output_source_id = self.task.data_storage_id
        self.input_data = self.task.input_data

        self.output_complete = False

    def _create_range(self):
        """
        Create an A1 style range for use in Google Sheets or Excel
        """
        self.current_function = 'create_range'
        self.current_action = 'Creating Range'
        #  Get _input data size attributes
        data_len = len(self.input_data)
        data_wid = max(len(x) for x in self.input_data)

        #  If Data Storage Type is Google Sheets get the row count of the sheet for clearing purposes
        if self.output_type.lower() == 'google sheets':
            gs = GSheets(self.output_source_id)
            gs.set_active_sheet(self.task.wb_sheet_name)
            row_count = gs.get_row_count()
            #   If no end row provided use row count for clear range
            if self.task.wb_end_row is not None:
                self.task.wb_end_row = int(self.task.wb_end_row)
            else:
                self.task.wb_end_row = row_count
        else:
            #  If no end row provided use data size to determine the end row
            if self.task.wb_end_row is not None:
                self.task.wb_end_row = int(self.task.wb_end_row)
            else:
                self.task.wb_end_row = data_len + (self.task.wb_start_row - 1)

        #  If no end column provided use the data width to determine the column
        if self.task.wb_end_column is not None:
            self.task.wb_end_column = int(self.task.wb_end_column)
        else:
            self.task.wb_end_column = data_wid + (self.task.wb_start_column - 1)

        #  Assign all ranges
        self.range_start = range_builder(self.task.wb_start_row, self.task.wb_start_column)
        self.range_end = range_builder(self.task.wb_end_row, self.task.wb_end_column)
        self.range_name = range_builder(self.task.wb_start_row, self.task.wb_start_column,
                                        end_row=self.task.wb_end_row, end_col=self.task.wb_end_column)

    def check_for_file(self):
        """
        Check if the file exist in File Storage
        """
        if self.task.file_name in os.listdir(self.task.file_storage):
            return True
        return False

    def download_file(self, id, file_name, file_location):
        """
        Open Google Drive a download required file
        :param id: The Google Drive ID to download
        :param file_name: What the file should be named once downloaded
        :param file_location: Where to store the file
        """
        file_exists = self.check_for_file()
        if not file_exists:
            gd = GDrive()
            gd.download_file(id, file_name, file_location)

    def _prep_data_for_gsheets(self):
        """
        Convert data in input_data to Json serializable values
        """
        try:
            for i, row in enumerate(self.input_data):
                for j, col in enumerate(row):
                    if isinstance(self.input_data[i][j], dt.datetime):
                        if self.input_data[i][j].hour > 0 or self.input_data[i][j].minute > 0:
                            self.input_data[i][j] = self.input_data[i][j].strftime('%m/%d/%Y %I:%M:%S %p')
                        else:
                            self.input_data[i][j] = self.input_data[i][j].strftime('%m/%d/%Y')
                    if isinstance(self.input_data[i][j], dt.date):
                        self.input_data[i][j] = self.input_data[i][j].strftime('%m/%d/%Y')
                    if isinstance(self.input_data[i][j], decimal.Decimal):
                        self.input_data[i][j] = str(self.input_data[i][j])
        except Exception as e:
            raise e

    def _google_sheet(self):
        self._prep_data_for_gsheets()
        try:
            gs = GSheets(self.output_source_id)
            gs.set_active_sheet(self.task.wb_sheet_name)
            self.gsheet_range = self.SheetRange(self.task.wb_start_row, self.task.wb_start_column,
                                                self.task.wb_end_row, self.task.wb_end_column)
            gs.update_sheet(self.input_data, range_data=self.gsheet_range, append=self.append)
            self.output_complete = True
        except Exception as e:
            raise e

    def _excel(self):
        if self.output_source_id:
            self.download_file(self.output_source_id, self.task.file_name, self.task.file_storage)
        self._create_range()
        try:
            excel = ExcelGenerator(self.input_data, self.task.file_name, self.task.wb_sheet_name,
                                   self.range_name, file_path=self.task.file_storage)
            excel.create_workbook()
            self.task.file_name = excel.file_name
            self.output_complete = True
        except Exception as e:
            raise e

    def _csv(self):
        try:
            csv_gen = CsvGenerator(self.input_data, self.task)
            csv_gen.create_csv()
            self.task.file_name = csv_gen.file_name
            self.output_complete = True
        except Exception as e:
            raise e

    def _data_warehouse(self):
        try:
            self.dw.insert_into_table(self.output_source_id, self.input_data,
                                      overwrite=not self.append, _meta_data_col=self.task.insert_timestamp)
            self.output_complete = True
        except Exception as e:
            raise e

    def _luna(self):
        if self.output_source_id[0] != '\"' \
                and self.output_source_id[-1] != '\"':
            self.output_source_id = '\"' + self.output_source_id.replace('\"', '') + '\"'

        self.luna_dw.open_connection()

        try:
            self.luna_dw.insert_into_table(self.output_source_id, self.input_data,
                                           overwrite=not self.append, _meta_data_col=self.task.insert_timestamp)
            self.output_complete = True
        except Exception as e:
            raise e
        finally:
            self.luna_dw.close_connection()

    def set_output(self):

        if self.output_type.lower() == 'csv':
            self._csv()

        elif self.output_type.lower() == 'excel':
            self._excel()

        elif self.output_type.lower() == 'google sheets':
            self._google_sheet()

        elif self.output_type.lower() == 'data warehouse':
            self._data_warehouse()

        elif self.output_type.lower() == 'luna':
            self._luna()
