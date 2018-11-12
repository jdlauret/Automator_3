import decimal

from BI.data_warehouse.connector import Snowflake
from BI.google.gdrive import GDrive
from BI.google.gsheets import GSheets, range_builder

from . import *


# %% Task
class Task:
    SheetRange = collections.namedtuple('SheetRange', 'start_row start_col end_row end_col')
    SheetRange.__new__.__defaults__ = (2, 1, 0, 0)

    def __init__(self, task_data, db_table='D_POST_INSTALL.T_AUTO_TASKS',
                 run_type='Automated', working_dir=None):
        """
        Task object used by Automator 3
        :param task_line: The line from the data table containing all the instructions for the task
        :param task_header: The column headers used to help find the items in the task line
        :param db_table: The table where the task data is stored
        :param run_type: Default to Automated, behavior changed for debugging if Testing used instead
        :param working_dir: The App's home directory
        """
        #  Create a Snowflake Connection
        self.dw = Snowflake()
        self.dw.set_user('JDLAURET')
        self.task_data = task_data
        self.db_table = db_table
        self.run_type = run_type
        self.metrics = None
        self.ready = False
        #  All of the statuses that should stop the task from working
        #  These are ignored if run_type is set to Testing
        self.run_statuses = [
            'operational',
            'paused',
        ]
        if run_type == 'Automated':
            self.DependentData = collections.namedtuple('DependentData', ','.join(self.task_data._fields))
        self.dynamic_name = None

        self.query = None

        self.input_complete = False
        self.output_complete = False
        self.task_complete = False

        self.input_data_header = None
        self.input_data = None

        self.range_start = 'A2'
        self.range_end = None
        self.range_name = None
        self.gsheet_range_data = None

        self.recurrence_day_of_month = None

        self.failed_attempts = 0
        self.main_dir = working_dir

        self.script_storage = os.path.join(self.main_dir, 'script_storage')
        self.file_storage = os.path.join(self.main_dir, 'file_storage')
        self.downloads = os.path.join(self.main_dir, 'downloads')

        self.current_function = None
        self.current_action = None

        self.input_time = None
        self.output_time = None
        self.upload_time = None

        self.task_table_column_names = []
        self.error_log = []

        #  Create all remaining attributes from Task table
        self.create_attributes()
        #  Create Task Logger object
        self.logger = None
        self.dependents_run = False

    def update_settings(self, settings):
        self.settings = settings

    def set_to_testing(self):
        self.run_type = 'Testing'

    def _create_logger(self):
        if not self.logger:
            self.logger = Logger(self)

    def _clean_task_data_header(self):
        for i, item in enumerate(self.task_table_column_names):
            if item.lower()[-1] == 'x':
                self.task_table_column_names[i] = item.lower().replace('x', '')
            else:
                self.task_table_column_names[i] = item.lower()

    def _refresh_attributes(self):
        """
        Grab task data from task table
        Assign changes to the changed attributes
        """
        self.current_function = '_refresh_attributes'
        query = 'SELECT * FROM {table} T WHERE T.ID = %s'.format(table=self.db_table)
        if self.id is not None:
            try:
                self.dw.execute_query(query, bindvars=[self.id])
                self.task_table_column_names = self.dw.column_names
                self._clean_task_data_header()
                TaskData = collections.namedtuple('TaskData', ' '.join(self.task_table_column_names))
                self.task_data = TaskData._make(self.dw.query_results[0])
            except Exception as e:
                raise e

        #  Task header used to create attribute names and matching task line used for the attribute value
        for name, value in self.task_data._asdict().items():
            try:
                current_value = self.__getattribute__(name)
                if current_value != value:
                    super().__setattr__(name, value)
            except AttributeError:
                super().__setattr__(name, value)
        #  Update logger object with new attribute data
        self._get_error_log()
        self.logger.update_task_data(self)

    def create_attributes(self):
        """
        Inital attribute setup
        Task header used to create attribute names and matching task line used for the attribute value
        """
        self.current_function = 'create_attributes'
        for name, value in self.task_data._asdict().items():
            self.__setattr__(name, value)

    def _get_error_log(self):
        query = '''
                SELECT * FROM D_POST_INSTALL.T_AUTO_ERROR_LOG 
                WHERE TASK_ID = {id}
                ORDER BY ERROR_TIMESTAMP DESC LIMIT 10
                '''.format(id=self.id)
        self.dw.execute_query(query)
        self.error_log = []
        ErrorLog = collections.namedtuple('ErrorLog', ' '.join(x.lower() for x in self.dw.column_names))
        for row in self.dw.query_results:
            error = ErrorLog._make(row)
            self.error_log.append(error)

    def _log_error(self, error):
        """
        Send error data to Logger object
        :param error: The error to log
        """
        if self.run_type != 'Testing':
            self.logger.log_error(self.current_function, self.current_action, error)

    def read_query(self):
        """
        Read a SQL query stored in Google Drive
        """
        self.current_function = 'read_query'
        self.current_action = 'GDrive - Read Drive File'
        try:
            #  Open Google Drive and read the sql file
            self.query = GDrive().read_drive_file(self.data_source_id)
        except Exception as e:
            #  Log Exception thrown
            self._log_error(e)

    def create_range(self):
        """
        Create an A1 style range for use in Google Sheets or Excel
        """
        self.current_function = 'create_range'
        self.current_action = 'Creating Range'
        #  Get input data size attributes
        data_len = len(self.input_data)
        data_wid = max(len(x) for x in self.input_data)

        #  If Data Storage Type is Google Sheets get the row count of the sheet for clearing purposes
        if self.data_storage_type.lower() == 'google sheets':
            row_count = GSheets(self.data_storage_id).get_row_count(self.wb_sheet_name)
            #   If no end row provided use row count for clear range
            if self.wb_end_row is not None:
                self.wb_end_row = int(self.wb_end_row)
            else:
                self.wb_end_row = row_count
        else:
            #  If no end row provided use data size to determine the end row
            if self.wb_end_row is not None:
                self.wb_end_row = int(self.wb_end_row)
            else:
                self.wb_end_row = data_len + (self.wb_start_row - 1)

        #  If no end column provided use the data width to determine the column
        if self.wb_end_column is not None:
            self.wb_end_column = int(self.wb_end_column)
        else:
            self.wb_end_column = data_wid + (self.wb_start_column - 1)

        #  Assign all ranges
        self.range_start = range_builder(self.wb_start_row, self.wb_start_column)
        self.range_end = range_builder(self.wb_end_row, self.wb_end_column)
        self.range_name = range_builder(self.wb_start_row, self.wb_start_column,
                                        end_row=self.wb_end_row, end_col=self.wb_end_column)

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

    def check_for_file(self):
        """
        Check if the file exist in File Storage
        """
        if self.file_name in os.listdir(self.file_storage):
            return True
        return False

    def _execute_command(self):
        """
        Execute a sql command
        """
        self.current_function = '_execute_command'
        self.current_action = 'SQL Command - Execute Command'
        try:
            self.dw.execute_sql_command(self.query)
        except Exception as e:
            #  Log Thrown Exception
            self._log_error(e)
            raise e

    def _multi_query_execution(self):
        """
        Separate a query using a semicolon ';'
        Then execute queries in order
        """
        self.current_function = '_multi_query_execution'
        multi_query_staging = self.query.split(';')
        for query in multi_query_staging:
            self.query = query
            self._execute_command()

    def _read_csv(self):
        """
        Open a csv and save the data as input_data
        """
        self.function_name = '_read_csv'
        reader = csv.reader(os.path.join(self.downloads, self.csv_name), dialect='excel')
        for row in reader:
            self.input_data.append(row)

    def create_dynamic_name(self):
        """
        Create a dynamic file name
        """
        self.function_name = 'create_dynamic_name'
        # Use before or after in after_before to setup new file name
        file_name, file_extension = os.path.splitext(self.file_name)
        #  If no file extension found assume type from data_storage_type
        if file_extension == '':
            if self.data_storage_type.lower() == 'excel':
                file_extension = '.xlsx'
            elif self.data_storage_type.lower() == 'csv':
                file_extension = '.csv'
        if self.after_before.lower() == 'after':
            self.file_name = file_name \
                             + ' ' \
                             + str(dt.datetime.strftime(dt.datetime.today(), self.dynamic_name)).strip() \
                             + file_extension
        else:
            self.file_name = str(dt.datetime.strftime(dt.datetime.today(), self.dynamic_name)).strip() \
                             + ' ' \
                             + file_name + file_extension

    def _prep_data_for_gsheets(self):
        """
        Convert data in input_data to Json Serializable values
        """
        self.current_function = '_prep_data_for_gsheets'
        self.current_action = 'Prepping data for Google Sheets'
        try:
            for i, row in enumerate(self.input_data):
                for j, col in enumerate(row):
                    if isinstance(col, dt.datetime):
                        if col.hour > 0 or col.minute > 0:
                            self.input_data[i][j] = col.strftime('%m/%d/%Y %I:%M:%S %p')
                        else:
                            self.input_data[i][j] = col.strftime('%m/%d/%Y')
                    if isinstance(col, dt.date):
                        self.input_data[i][j] = col.strftime('%m/%d/%Y')
                    if isinstance(col, decimal.Decimal):
                        self.input_data[i][j] = str(col)
        except Exception as e:
            self._log_error(e)

    def _check_input_data(self):
        if not self.input_data:
            #  If no data is returned, check to see if this is an error
            #  and log error if needed and then email the task owner
            if self.no_data_is_error:
                self._log_error('No Data Returned From Query')
                self.logger.send_error_email()
            #  If no data is ok check if owner would like to be notified
            #  Notify owner if needed
            #  Mark entire task as completed
            if self.no_data_notification and not self.no_data_is_error:
                self.logger.send_no_data_notification()
                self.task_complete = True
            else:
                self.task_complete = True

    def _size_gsheet_input_data(self):
        self.current_function = '_size_gsheet_input_data'
        self.current_action = 'Input - Gsheet Sizing Data'
        if not self.gsheet_range:
            self.gsheet_range = self.SheetRange
        if not all(x for x in self.gsheet_range._asdict().items()):
            del self.input_data[0]
        if self.gsheet_range.start_row:
            self.gsheet_range = self.gsheet_range._replace(start_row=self.gsheet_range.start_row - 1)
        if self.gsheet_range.start_col:
            self.gsheet_range = self.gsheet_range._replace(start_col=self.gsheet_range.start_col - 1)
        self.input_data = self.input_data[self.gsheet_range.start_row:self.gsheet_range.end_row]
        for i, row in enumerate(self.input_data):
            self.input_data[i] = row[self.gsheet_range.start_col:self.gsheet_range.end_col]

    def _sql_query_input(self):
        self.read_query()
        if self.query:
            try:
                self.current_action = 'SQL Query - Execute Query'
                self.dw.execute_query(self.query)

                self.input_data = self.dw.query_results
                self.input_data_header = self.dw.column_names
                self.input_complete = True
                self._check_input_data()
            except Exception as e:
                #  Log any exception thrown
                self._log_error(e)

    def _sql_command_input(self):
        #  Check if multiple commands are present
        #  Execute all commands
        self.read_query()
        try:
            query_count = self.query.count(';')
            if query_count > 1:
                self._multi_query_execution()
            else:
                self._execute_command()
            # Mark task as complete
            self.task_complete = True
        except Exception as e:
            #  Log any exception thrown
            self._log_error(e)

    def _python_script_input(self):
        self.current_action = 'Python - Run Script'
        try:
            p_script = PythonScript(self)
            p_script.run_script()
            #  If successful run is logged then mark task complete
            self.task_complete = p_script.successful_run
            if not self.task_complete:
                #  If task complete is false log the Return Code and Stream data, then log error
                error = 'Return Code: {rc} Stream Data: {stream_data}'.format(rc=p_script.rc,
                                                                              stream_data=p_script.stream_data)
                self._log_error(error)
        except Exception as e:
            #  Log and exception thrown
            self._log_error(e)

    def _csv_input(self):
        self.current_action = 'Input - Read CSV'
        #  Create file name
        if '.csv' not in self.file:
            self.csv_name = self.file_name + '.csv'
        else:
            self.csv_name = self.file_name
        #  Download file
        self.download_file(self.data_source_id, self.name, self.downloads)
        try:
            #  Open csv and store data
            self._read_csv()
        except Exception as e:
            #  Log any exception thrown
            self._log_error(e)

    def _csv_output(self):
        try:
            self.current_function = 'set_output_data'
            self.current_action = 'Output - Create CSV'
            csv = CsvGenerator(self.input_data, self)
            csv.create_csv()
            self.file_name = csv.file_name
            self.output_complete = True
        except Exception as e:
            self._log_error(e)

    def _google_sheet_input(self):
        self.current_action = 'Input - Init GSheets'
        try:
            gs = GSheets(self.data_source_id)
            gs.set_active_sheet(self.wb_sheet_name)
            gs.get_sheet_data()
            self.input_data = gs.results
            if any([self.wb_start_row, self.wb_start_column, self.wb_end_row, self.wb_end_column]):
                self.gsheet_range = self.SheetRange(self.wb_start_row, self.wb_start_column,
                                                    self.wb_end_row, self.wb_end_column)
            else:
                self.gsheet_range = self.SheetRange()
            self._size_gsheet_input_data()
            self.current_function = 'get_input_data'
            self.input_complete = True
            self._check_input_data()
        except Exception as e:
            #  Log and exception thrown
            self._log_error(e)

    def _google_sheet_output(self):
        self._prep_data_for_gsheets()
        try:
            self.current_function = 'set_output_data'
            self.current_action = 'Output - Init Gsheets'
            gs = GSheets(self.data_storage_id)
            gs.set_active_sheet(self.wb_sheet_name)
            self.gsheet_range = self.SheetRange(self.wb_start_row, self.wb_start_column,
                                                self.wb_end_row, self.wb_end_column)
            gs.update_sheet(self.input_data, range_data=self.gsheet_range, append=self.append)
            self.task_complete = True
        except Exception as e:
            self._log_error(e)

    def _excel_output(self):
        if self.data_storage_id:
            self.download_file(self.data_storage_id, self.file_name, self.file_storage)
        self.create_range()
        try:
            self.current_function = 'set_output_data'
            self.current_action = 'Output - Excel Init'
            excel = ExcelGenerator(self.input_data, self.file_name, self.wb_sheet_name,
                                   self.range_name, file_path=self.file_storage)
            self.current_action = 'Output - Create Excel Workbook'
            excel.create_workbook()
            self.file_name = excel.file_name
            self.output_complete = True
        except Exception as e:
            self._log_error(e)

    def get_input_data(self):
        """
        Check Data Source and execute task type
        """
        self.current_function = 'get_input_data'
        #  Log start of input action
        input_time_start = dt.datetime.now()

        if self.data_source.lower() == 'sql':
            #  If data source is SQL.  Execute SQL query and return results to input_data
            self._sql_query_input()

        elif self.data_source.lower() == 'sql command':
            #  If data source is a SQL command

            self._sql_command_input()

        elif self.data_source.lower() == 'python':
            #  If data source is python, execute the python script
            self._python_script_input()

        elif self.data_source.lower() == 'csv':
            self._csv_input()

        elif self.data_source.lower() == 'dialer':
            self.current_action = 'Input - Read Dialer'
            #  TODO Create Dialer Handling
            pass

        elif self.data_source.lower() == 'google sheets':
            self._google_sheet_input()

        #  Create input time log
        self.input_time = (dt.datetime.now() - input_time_start).seconds

    def set_output_data(self):
        self.current_function = 'set_output_data'
        output_time_start = dt.datetime.now()

        if self.dynamic_name is not None \
                and (self.data_storage_type.lower() == 'csv'
                     or self.data_storage_type.lower() == 'excel'):
            self.create_dynamic_name()

        if self.input_complete:
            if self.data_storage_type.lower() == 'csv':
                self._csv_output()

            elif self.data_storage_type.lower() == 'excel':
                self._excel_output()

            elif self.data_storage_type.lower() == 'google sheets':
                self._google_sheet_output()

            elif self.data_storage_type.lower() == 'data warehouse':
                self.current_action = 'Output - Insert Data Warehouse'
                try:
                    if self.data_source.lower() == 'csv':
                        self.dw.insert_csv_into_table(self.data_storage_id, self.downloads, self.csv_name)
                    else:
                        self.dw.insert_into_table(self.data_storage_id, self.input_data,
                                                  overwrite=not self.append, _meta_data_col=self.insert_timestamp)
                    self.task_complete = True
                except Exception as e:
                    self._log_error(e)
            self.output_time = (dt.datetime.now() - output_time_start).seconds

    def upload_files(self):
        self.current_function = 'upload_files'
        upload_time_start = dt.datetime.now()
        if self.storage_type == 'Google Drive':
            for file in os.listdir(self.file_storage):
                if file == self.file_name:
                    try:
                        self.current_action = 'Upload File'
                        drive = GDrive()
                        file_path = self.file_storage
                        drive.upload_file(self.file_name,
                                          file_path,
                                          self.storage_id,
                                          replace_existing=True)
                        self.task_complete = True
                    except Exception as e:
                        self._log_error(e)
        self.upload_time = (dt.datetime.now() - upload_time_start).seconds

    def update_last_run(self):
        self.current_function = 'update_last_run'
        if self.run_requested.lower() != 'true':
            query = 'UPDATE {table}\n' \
                    'SET LAST_RUN = current_timestamp::timestamp_ntz\n' \
                    'WHERE ID = %s'.format(table=self.db_table)
            self.dw.execute_sql_command(query, bindvars=[str(self.id)])

    def _update_last_attempt(self):
        self.current_function = 'update_last_run'
        query = 'UPDATE {table}\n' \
                'SET LAST_ATTEMPT = current_timestamp::timestamp_ntz\n' \
                'WHERE ID = %s'.format(table=self.db_table)
        self.dw.execute_sql_command(query, bindvars=[str(self.id)])

    def update_run_requested(self):
        self.current_function = 'update_run_requested'
        query = 'UPDATE {table}\n' \
                'SET RUN_REQUESTED = \'FALSE\'\n' \
                'WHERE ID = %s'.format(table=self.db_table)
        self.dw.execute_sql_command(query, bindvars=[str(self.id)])

    def disable_task(self):
        self.current_function = 'disable_task'
        query = 'UPDATE {table}\n' \
                'SET OPERATIONAL = \'Disabled\'\n' \
                'WHERE ID = %s'.format(table=self.db_table)
        self.dw.execute_sql_command(query, bindvars=[str(self.id)])

    def pause_task(self):
        self.current_function = 'disable_task'
        query = 'UPDATE {table}\n' \
                'SET OPERATIONAL = \'Paused\'\n' \
                'WHERE ID = %s'.format(table=self.db_table)
        self.dw.execute_sql_command(query, bindvars=[str(self.id)])

    def resume_task(self):
        self.current_function = 'disable_task'
        query = 'UPDATE {table}\n' \
                'SET OPERATIONAL = \'Operational\'\n' \
                'WHERE ID = %s'.format(table=self.db_table)
        self.dw.execute_sql_command(query, bindvars=[str(self.id)])

    def _reset_flags(self):
        # Reset flags
        self.task_complete = False
        self.input_complete = False
        self.output_complete = False
        self.task_complete = False
        self.input_data_header = None
        self.input_data = None

    def _dependency_check(self):
        run_dependents = []

        if self.dependencies:
            query = '''SELECT * FROM D_POST_INSTALL.T_AUTO_TASKS WHERE ID in ({id})'''.format(id=self.dependencies)
            self.dw.execute_query(query)
            for result in self.dw.query_results:
                test = not recur_test(self.DependentData._make(result))
                run_dependents.append(test)
            return all(x for x in run_dependents)
        else:
            return True

    def run_eval(self):
        self.ready = False
        if self.run_type == 'Testing':
            self.ready = True

        else:
            automated_run = recur_test(self)
            self.dependents_run = self._dependency_check()
            status_run = self.operational.lower() in self.run_statuses
            run_requested = self.run_requested.lower() == 'true'

            if automated_run and run_requested:
                self.update_run_requested()
                self.run_requested = 'FALSE'
                run_requested = False

            self.ready = (automated_run and self.dependents_run and status_run) or run_requested

    def run_task(self):
        self._create_logger()
        try:
            self.dw.open_connection()
            self.current_function = 'run_task'

            self._refresh_attributes()

            self.run_eval()

            if self.ready:
                if self.run_requested.lower() == 'true':
                    print('    IN PROGRESS - Manual Request -', self.name)
                elif self.run_type == 'Testing':
                    print('    IN PROGRESS - Testing -', self.name)
                else:
                    print('    IN PROGRESS - Automated - Priority:', self.priority, '-', self.name)
                self._update_last_attempt()
                self.get_input_data()
                if self.input_complete and self.input_data:
                    self.set_output_data()
                    if self.output_complete:
                        self.upload_files()

                if self.task_complete \
                        and self.run_type.lower() == 'automated':
                    if self.run_requested.lower() == 'true':
                        self.update_run_requested()
                    print('    COMPLETED -', self.name)
                    self.metrics = TaskMetrics(self)
                    self.metrics.submit_task_time()
                    self.update_last_run()

                elif self.task_complete and self.run_type.lower() == 'testing':
                    print('    COMPLETED -', self.name)
                else:
                    print('    NOT COMPLETED -', self.name, '- ERROR -', self.error_log[0].error)

                if self.logger.paused and self.task_complete:
                    self.resume_task()
            if not self.dependents_run \
                    and (not self.logger.disabled
                         or not self.logger.paused):
                self.current_action = 'Dependency Check'
                self.current_function = 'run'
                self._log_error('Dependent Task(s) did not run')
        except Exception as e:
            print('Exception {}:'.format(self.name), str(e))
            self._log_error(str(e))
            raise e

        finally:
            self._reset_flags()
            self.dw.close_connection()
