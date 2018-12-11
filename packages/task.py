from BI.data_warehouse import Snowflake, SnowflakeV2, SnowflakeConnectionHandlerV2
from BI.google import GDrive

from packages.IO import TaskInput, TaskOutput, Upload, TaskConsole
from . import *


# %% Task
class Task:
    SheetRange = collections.namedtuple('SheetRange', 'start_row start_col end_row end_col')
    SheetRange.__new__.__defaults__ = (2, 1, 0, 0)

    def __init__(self, task_data, connection,
                 run_type='Automated', working_dir=None,):
        """
        Task object used by Automator 3
        :param task_data: A name tuple containing the task data from the task table
        :param db_table: The table where the task data is stored
        :param run_type: Default to Automated, behavior changed for debugging if Testing used instead
        :param working_dir: The App's home directory
        """
        #  Create a Snowflake Connection
        self.db_connection = connection
        self.dw = SnowflakeV2(self.db_connection)
        self.dw.set_user('JDLAURET')

        # Console Output
        self.console = None

        self.task_data = task_data
        self.db_table = 'D_POST_INSTALL.T_AUTO_TASKS'
        self.run_type = run_type
        self.metrics = None

        #  All of the statuses that should stop the task from working
        #  These are ignored if run_type is set to Testing

        if run_type == 'Automated':
            self.DependentData = collections.namedtuple('DependentData', ','.join(self.task_data._fields))
        self.dynamic_name = None

        self.query = None

        # Task Flags
        self.ready = False
        self.input_complete = False
        self.output_complete = False
        self.upload_complete = False
        self.task_complete = False
        self.dependents_run = False

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

        self.run_statuses = ['operational', 'paused', 'disabled']
        self.require_output = ['sql', 'google sheets', 'csv',]
        self.require_upload = ['csv', 'excel',]

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
        query = 'SELECT * FROM {table} T WHERE T.ID = {id}'.format(table=self.db_table, id=self.id)
        if self.id is not None:
            try:
                if self.dw.connection.connection:
                    if self.dw.connection.connection.is_closed():
                        self.dw.open_connection()
                self.dw.execute_query(query)
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
        # Refresh error log data
        self._get_error_log()
        #  Update logger object with new attribute data
        self.logger.update_task_data(self)
        # Create Console class
        self.console = TaskConsole(self)

    def create_attributes(self):
        """
        Initial attribute setup
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
            if error.run_type is None:
                error = error._replace(run_type='')
            self.error_log.append(error)

    def _log_error(self, error):
        """
        Send error data to Logger object
        :param error: The error to log
        """
        self.logger.log_error(self.current_function, self.current_action, error)

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

    def _verify_input_data(self):
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

    def _update_last_run(self):
        self.current_function = '_update_last_run'
        if self.run_requested.lower() != 'true':
            query = '''UPDATE {table}\n
            SET LAST_RUN = current_timestamp::timestamp_ntz\n
            WHERE ID = {id}'''.format(table=self.db_table, id=self.id)
            if self.db_table is None or self.id is None:
                print(self.task_name, '\ndb_table:', self.db_table, '\nid:', self.id)
            self.dw.execute_sql_command(query)

    def _update_last_attempt(self):
        self.current_function = '_update_last_run'
        query = '''UPDATE {table}\n
        SET LAST_ATTEMPT = current_timestamp::timestamp_ntz\n
        WHERE ID = {id}'''.format(table=self.db_table, id=self.id)
        self.dw.execute_sql_command(query)

    def update_run_requested(self):
        self.current_function = 'update_run_requested'
        query = '''UPDATE {table}\n
        SET RUN_REQUESTED = \'FALSE\'\n
        WHERE ID = {id}'''.format(table=self.db_table, id=self.id)
        self.dw.execute_sql_command(query)

    def disable_task(self):
        self.current_function = 'disable_task'
        query = '''UPDATE {table}\n
        SET OPERATIONAL = \'Disabled\'\n
        WHERE ID = {id}'''.format(table=self.db_table, id=self.id)
        self.dw.execute_sql_command(query)

    def pause_task(self):
        self.current_function = 'pause_task'
        query = '''UPDATE {table}\n
        SET OPERATIONAL = \'Paused\'\n
        WHERE ID = {id}'''.format(table=self.db_table, id=self.id)
        self.dw.execute_sql_command(query)

    def _resume_task(self):
        if (self.logger.paused or self.logger.disabled)\
                and self.task_complete:
            self.current_function = '_resume_task'
            if self.db_table is None or self.id is None:
                print(self.task_name, '\ndb_table:', self.db_table, '\nid:', self.id)
            query = '''UPDATE {table}\n
            SET OPERATIONAL = \'Operational\'\n
            WHERE ID = {id}'''.format(table=self.db_table, id=self.id)
            self.dw.execute_sql_command(query)

    def _reset_flags(self):
        # Reset flags
        self.task_complete = False
        self.input_complete = False
        self.output_complete = False
        self.upload_complete = False
        self.dependents_run = False
        self.ready = False
        self.input_data = None
        self.input_data_header = None

    def _dependency_check(self):
        run_dependents = []

        if self.dependencies:
            query = '''SELECT * FROM D_POST_INSTALL.T_AUTO_TASKS WHERE ID in ({id})'''.format(id=self.dependencies)
            self.dw.execute_query(query)
            for result in self.dw.query_results:
                dependent_data = self.DependentData._make(result)
                test = not recur_test_v2(dependent_data.last_run,
                                         dependent_data.auto_recurrence,
                                         hour=dependent_data.recurrence_hour
                                         if dependent_data.recurrence_hour else 0,
                                         day=dependent_data.auto_recurrence_day
                                         if dependent_data.auto_recurrence_day else 'Monday')
                run_dependents.append(test)
            return all(x for x in run_dependents)
        else:
            return True

    def _verify_task_complete(self):
        if self.input_complete and self.data_source.lower() not in self.require_output:
            self.task_complete = True
        elif self.input_complete and self.output_complete and self.data_storage_type.lower() not in self.require_upload:
            self.task_complete = True
        elif self.input_complete and self.output_complete and self.upload_complete:
            self.task_complete = True

    def _input(self):
        """
        Check Data Source and execute task type
        """
        self.current_function = 'input'
        #  Log start of input action
        input_time_start = dt.datetime.now()
        try:
            input_handler = TaskInput(self)
            input_handler.get_input()
            self.input_data = input_handler.input_data
            self.input_data_header = input_handler.input_data_header
            self._verify_input_data()
            self.input_complete = input_handler.input_complete
        except Exception as e:
            self._log_error(e)
        #  Create input time log
        self.input_time = (dt.datetime.now() - input_time_start).seconds

    def _output(self):
        self.current_function = '_output'
        output_time_start = dt.datetime.now()
        if self.dynamic_name is not None \
                and (self.data_storage_type.lower() == 'csv'
                     or self.data_storage_type.lower() == 'excel'):
            self.create_dynamic_name()

        try:
            output_handler = TaskOutput(self)
            output_handler.set_output()
            self.output_complete = output_handler.output_complete
        except Exception as e:
            self._log_error(e)

        self.output_time = (dt.datetime.now() - output_time_start).seconds

    def _upload(self):
        self.current_function = '_upload'
        upload_time_start = dt.datetime.now()
        try:
            upload_handler = Upload(self)
            upload_handler.upload()
            self.upload_complete = upload_handler.upload_complete
        except Exception as e:
            self._log_error(e)
        self.upload_time = (dt.datetime.now() - upload_time_start).seconds

    def _run_eval(self):
        self.ready = False
        if self.run_type.lower() != 'cycle':
            self.dependents_run = self._dependency_check()
            if self.logger.paused:
                recurrence_test = recur_test_v2(self.last_attempt, 'Hourly')
            elif self.logger.disabled:
                recurrence_test = recur_test_v2(self.last_attempt, 'Daily')
            else:
                recurrence_test = recur_test_v2(self.last_run,
                                                self.auto_recurrence,
                                                hour=self.recurrence_hour if self.recurrence_hour else 0,
                                                day=self.auto_recurrence_day if self.auto_recurrence_day else 'Monday')
            status_run = self.operational.lower() in self.run_statuses

            run_requested = self.run_requested.lower() == 'true'

            if recurrence_test and run_requested:
                self.update_run_requested()
                self.run_requested = 'FALSE'
                run_requested = False

            self.ready = all([self.dependents_run, recurrence_test, status_run]) or run_requested
        if self.run_type.lower() in ('testing', 'cycle'):
            self.ready = True

    def _execute_task(self):
        self._input()

        if self.input_complete \
                and self.input_data \
                and self.data_source.lower() in self.require_output:
            self._output()

        if self.output_complete \
                and self.data_storage_type.lower() in self.require_upload:
            self._upload()

    def _update_task(self):
        if self.run_type.lower() not in ('testing', 'cycle'):
            if self.task_complete:
                if self.run_requested.lower() == 'true':
                    self.update_run_requested()
                self.metrics = TaskMetrics(self)
                self.metrics.submit_task_time()
                self._update_last_run()
            else:
                # If dependents failed to run and task is not already paused or disabled.  Log Dependency failure
                if not self.dependents_run \
                        and (not self.logger.disabled
                             or not self.logger.paused):
                    self.current_action = 'Dependency Check'
                    self.current_function = 'run'
                    self._log_error('Dependent Task(s) did not run')

    def run_task(self):
        """
        Run Task
        Reads task instructions from the database
        Evaluates new instructions
        Runs the task if needed
        """
        # Create a new logger for the task instance
        self._create_logger()
        try:
            self.current_function = 'run_task'
            # Get new instructions and set attributes
            self._refresh_attributes()

            # Check to see if Task should be run and sets ready to True or False
            self._run_eval()

            if self.ready:
                # Print task startup information
                self.console.task_startup()

                # Update the latest attempt to run the task
                self._update_last_attempt()

                # Run the task
                self._execute_task()

                # Verify the task has been completed
                self._verify_task_complete()

                # Update all necessary task info in Database
                self._update_task()

                # Print task shutdown information
                self.console.task_shutdown()

            # Check if paused task should be be set back to Operational
            self._resume_task()

        except Exception as e:
            print('Exception on {}:'.format(self.name), str(e))
            self._log_error(str(e))
            raise e

        finally:
            if self.run_type.lower() == 'testing':
                self.console.print_test_results()
            # Reset instance variables
            self._reset_flags()
