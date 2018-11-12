from . import *


class TaskInput:

    def __init__(self, task):
        self.task = task
        self.dw = self.task.dw
        self.SheetRange = self.task.SheetRange
        self.input_type = self.task.data_source
        self.input_source_id = self.task.data_source_id

        self.query = None

        self.input_data = []
        self.input_data_header = []
        self.input_complete = False

    def _read_query(self):
        """
        Read a SQL query stored in Google Drive
        """
        self.current_function = 'read_query'
        self.current_action = 'GDrive - Read Drive File'
        try:
            #  Open Google Drive and read the sql file
            self.query = GDrive().read_drive_file(self.input_source_id)
        except Exception as e:
            raise e

    def _multi_query_execution(self):
        """
        Seperate a query using a semicolon ';'
        Then execute queries in order
        """
        self.current_function = '_multi_query_execution'
        multi_query_staging = self.query.split(';')
        for query in multi_query_staging:
            self.query = query
            self._execute_command()

    def _execute_command(self):
        """
        Execute a sql command
        """
        self.current_function = '_execute_command'
        self.current_action = 'SQL Command - Execute Command'
        try:
            self.dw.execute_sql_command(self.query)
        except Exception as e:
            raise e

    def _sql_query(self):
        self.task.read_query()
        if self.task.query:
            try:
                self.current_action = 'SQL Query - Execute Query'
                self.dw.execute_query(self.task.query)

                self.input_data = self.dw.query_results
                self.input_data_header = self.dw.column_names
                self.input_complete = True
            except Exception as e:
                raise e

    def _sql_command(self):
        #  Check if multiple commands are present
        #  Execute all commands
        self._read_query()
        try:
            query_count = self.query.count(';')
            if query_count > 1:
                self._multi_query_execution()
            else:
                self._execute_command()
            # Mark task as complete
            self.task_complete = True
        except Exception as e:
            raise e

    def _python_script(self):
        try:
            p_script = PythonScript(self)
            p_script.run_script()
            #  If successful run is logged then mark task complete
            self.task_complete = p_script.successful_run
            if not self.task_complete:
                #  If task complete is false log the Return Code and Stream data, then log error
                error = 'Return Code: {rc} Stream Data: {stream_data}'.format(rc=p_script.rc,
                                                                              stream_data=p_script.stream_data)
                raise error
        except Exception as e:
            raise e

    def _google_sheet(self):
        try:
            gs = GSheets(self.input_source_id)
            gs.set_active_sheet(self.task.wb_sheet_name)
            gs.get_sheet_data()
            self.input_data = gs.results
            if any([self.task.wb_start_row, self.task.wb_start_column, self.task.wb_end_row, self.task.wb_end_column]):
                self.gsheet_range = self.SheetRange(self.task.wb_start_row, self.task.wb_start_column,
                                                    self.task.wb_end_row, self.task.wb_end_column)
            else:
                self.gsheet_range = self.SheetRange()
            self._size_gsheet_input_data()
            self.current_function = 'get_input_data'
            self.input_complete = True
        except Exception as e:
            raise e

    def _size_gsheet_input_data(self):
        if not self.gsheet_range:
            self.gsheet_range = self.task.SheetRange
        if not all(x for x in self.gsheet_range._asdict().items()):
            del self.input_data[0]
        if self.gsheet_range.start_row:
            self.gsheet_range = self.gsheet_range._replace(start_row=self.gsheet_range.start_row - 1)
        if self.gsheet_range.start_col:
            self.gsheet_range = self.gsheet_range._replace(start_col=self.gsheet_range.start_col - 1)
        self.input_data = self.input_data[self.gsheet_range.start_row:self.gsheet_range.end_row]
        for i, row in enumerate(self.input_data):
            self.input_data[i] = row[self.gsheet_range.start_col:self.gsheet_range.end_col]

    def _csv(self):
        self.current_action = 'Input - Read CSV'
        #  Create file name
        if '.csv' not in self.task.file:
            self.csv_name = self.task.file_name + '.csv'
        else:
            self.csv_name = self.task.file_name
        #  Download file
        self.task.download_file(self.input_source_id, self.task.name, self.task.downloads)
        try:
            #  Open csv and store data
            self._read_csv()
        except Exception as e:
            raise e

    def _read_csv(self):
        """
        Open a csv and save the data as input_data
        """
        self.function_name = '_read_csv'
        reader = csv.reader(os.path.join(self.task.downloads, self.csv_name), dialect='excel')
        for row in reader:
            self.input_data.append(row)

    def get_input(self):

        if self.input_type.lower() == 'sql':
            #  If data source is SQL.  Execute SQL query and return results to input_data
            self._sql_query()

        elif self.input_type.lower() == 'sql command':
            #  If data source is a SQL command
            self._sql_command()

        elif self.input_type.lower() == 'python':
            #  If data source is python, execute the python script
            self._python_script()

        elif self.input_type.lower() == 'csv':
            self._csv()

        elif self.input_type.lower() == 'dialer':
            #  TODO Create Dialer Handling
            pass

        elif self.input_type.lower() == 'google sheets':
            self._google_sheet()
