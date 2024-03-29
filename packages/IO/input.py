from . import *


class TaskInput:

    def __init__(self, task):
        self.task = task
        self.database = self.task.sql_database
        self.dw = None
        if self.database:
            if self.database.lower() == 'data warehouse':
                self.dw = self.task.dw
            elif self.database.lower() == 'luna':
                self.dw = Postgres()
                self.dw.login(os.environ.get('POSTGRES_HOST'))
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
        try:
            #  Open Google Drive and read the sql file
            self.query = GDrive().read_drive_file(self.input_source_id)
        except Exception as e:
            raise e

    def _multi_query_execution(self):
        """
        Separate a query using a semicolon ';'
        Then execute queries in order
        """
        multi_query_staging = self.query.split(';')
        for query in multi_query_staging:
            self.query = query
            self._execute_command()

    def _execute_command(self):
        """
        Execute a sql command
        """
        try:
            self.dw.execute_sql_command(self.query)
        except Exception as e:
            raise e

    def _sql_query(self):
        self._read_query()
        if self.query:
            try:
                if self.dw:
                    self.dw.execute_query(self.query)

                    self.input_data = self.dw.query_results
                    self.input_data_header = self.dw.column_names
                    self.input_complete = True
                else:
                    self.task._log_error('Read Query', 'SQL_DATABASE in T_AUTO_TASKS cannot be Null for this task')
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
            self.input_complete = True
        except Exception as e:
            raise e

    def _python_script(self):
        try:
            p_script = PythonScript(self.task)
            p_script.run_script()
            #  If successful run is logged then mark task complete
            self.input_complete = p_script.successful_run
            if not self.input_complete:
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
        #  Create file name
        if '.csv' not in self.task.file_name:
            self.csv_name = self.task.file_name + '.csv'
        else:
            self.csv_name = self.task.file_name
        self.task.file_name = self.csv_name
        #  Download file
        self.download_file(self.input_source_id, self.csv_name, self.task.downloads)
        try:
            #  Open csv and store data
            self._read_csv()
            self.input_data_header = self.input_data[0]
            del self.input_data[0]
            self.input_complete = True
        except Exception as e:
            raise e

    def _read_csv(self):
        """
        Open a csv and save the data as input_data
        """
        self.function_name = '_read_csv'
        with open(os.path.join(self.task.downloads, self.csv_name)) as csv_file:
            reader = csv.reader(csv_file, dialect='excel')
            for row in reader:
                self.input_data.append(row)

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
        if not self.check_for_file():
            gd = GDrive()
            gd.download_file(id, file_name, file_location)

    def get_input(self):
        try:
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
        except Exception as e:
            raise e
        finally:
            if self.database:
                if self.database.lower() == 'luna':
                    self.dw.close_connection()
