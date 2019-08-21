import os
import collections
import datetime as dt
from BI.data_warehouse import SnowflakeV2
from . import recur_test_v2, TaskMetrics, TaskOutput, TaskInput, TaskConsole, Upload, TaskTable, Logger


class Task:
    db_table = 'D_POST_INSTALL.T_AUTO_TASKS'
    run_statuses = ['operational', 'paused', 'disabled']
    require_output = ['sql', 'google sheets', 'csv', ]
    require_upload = ['csv', 'excel', ]

    SheetRange = collections.namedtuple('SheetRange', 'start_row start_col end_row end_col')
    SheetRange.__new__.__defaults__ = (2, 1, 0, 0)

    def __init__(self, task_data, connection, run_type='Automated', working_dir=None, **kwargs):
        self.connection = connection
        self.dw = SnowflakeV2(self.connection)
        self.task_data = task_data
        self.run_type = run_type
        self.kwargs = kwargs
        self.refresh = self.kwargs.get('refresh')
        self.print_lock = self.kwargs.get('print_lock')

        self.main_dir = working_dir
        self.script_storage = os.path.join(self.main_dir, 'script_storage')
        self.file_storage = os.path.join(self.main_dir, 'local_storage')
        self.downloads = os.path.join(self.main_dir, 'downloads')

        self.MetaData = collections.namedtuple('MetaData', 'last_run last_attempt')
        self.last_attempt_update = False

        self.task_complete = False
        self.input_complete = False
        self.output_complete = False
        self.upload_complete = False
        self.input_time = None
        self.output_time = None
        self.upload_time = None

        self.dependents_run = False
        self.ready = False
        self.input_data = []
        self.input_data_header = None
        self.input_data_verified = False
        self.range_start = 'A2'
        self.range_end = None
        self.range_name = None
        self.gsheet_range_data = None

        self.refresh_attempt = 0
        self.refresh_limit = 3

        if not self.refresh:
            self._create_attributes()

        self.TaskTable = TaskTable(self)
        self.TaskConsole = TaskConsole(self)
        self.Logger = Logger(self)

    def _create_attributes(self):
        """
        Initial attribute setup
        Task header used to create attribute names and matching task line used for the attribute value
        """
        for name, value in self.task_data._asdict().items():
            self.__setattr__(name, value)
        self.MetaData = self.MetaData._make([self.last_run, self.last_attempt])

    def _log_error(self, action, error):
        """
        Send error data to Logger object
        :param error: The error to log
        """
        self.Logger.log_error(action, error)

    def _clean_task_data_header(self):
        for i, item in enumerate(self.task_table_column_names):
            if item.lower()[-1] == 'x':
                self.task_table_column_names[i] = item.lower().replace('x', '')
            else:
                self.task_table_column_names[i] = item.lower()

    def _dependency_check(self):
        run_dependents = []
        if self.run_type == 'Automated' and self.dependencies is not None:
            self.DependentData = collections.namedtuple('DependentData', ','.join(self.task_data._fields))
            query = '''SELECT * FROM D_POST_INSTALL.T_AUTO_TASKS WHERE ID in ({id})'''.format(id=self.dependencies)
            self.dw.execute_query(query)
            for result in self.dw.query_results:
                dependent_data = self.DependentData._make(result)
                recurrence_check = not recur_test_v2(dependent_data.last_run,
                                                     dependent_data.auto_recurrence,
                                                     hour=dependent_data.recurrence_hour,
                                                     day=dependent_data.auto_recurrence_day,
                                                     day_of_month=dependent_data.recurrence_day_of_month)
                run_dependents.append(recurrence_check)
            dependents_run = all(x for x in run_dependents)
            return dependents_run
        else:
            return True

    def _verify_task_complete(self):
        if self.input_complete \
                and self.data_source.lower() not in self.require_output:
            self.task_complete = True
        elif (self.input_complete and self.data_source.lower() in self.require_output) \
                and (self.output_complete and self.data_storage_type.lower() not in self.require_upload):
            self.task_complete = True
        elif (self.input_complete and self.data_source.lower() in self.require_output) \
                and (self.output_complete and self.data_storage_type.lower() in self.require_upload) \
                and self.upload_complete:
            self.task_complete = True

    def _verify_input_data(self):
        if len(self.input_data) == 0:
            #  If no data is returned, check to see if this is an error
            #  and log error if needed and then email the task owner
            #  If no data is ok check if owner would like to be notified
            #  Notify owner if needed then mark task as complete
            if self.no_data_is_error:
                self._log_error('Data Verification', 'No Data Returned From Query')
                self.Logger.send_error_email()
            elif self.no_data_notification:
                self.Logger.send_no_data_notification()
                if self.data_storage_type and self.data_storage_type.lower() in ['google sheets', 'excel']:
                    self.input_data_verified = True
                else:
                    self.task_complete = True
            else:
                self.task_complete = True
        else:
            self.input_data_verified = True

    def _input(self):
        """
        Check Data Source and execute task type
        """
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
            self._log_error('Task Input', e)
        #  Create input time log
        self.input_time = (dt.datetime.now() - input_time_start).seconds

    def _output(self):
        output_time_start = dt.datetime.now()
        # TODO put the dynamic name creation into TaskOutput
        if self.dynamic_name is not None \
                and (self.data_storage_type.lower() == 'csv'
                     or self.data_storage_type.lower() == 'excel'):
            self.create_dynamic_name()

        try:
            output_handler = TaskOutput(self)
            output_handler.set_output()
            self.output_complete = output_handler.output_complete
        except Exception as e:
            self._log_error('Task Output', e)

        self.output_time = (dt.datetime.now() - output_time_start).seconds

    def _upload(self):
        upload_time_start = dt.datetime.now()
        try:
            upload_handler = Upload(self)
            upload_handler.upload()
            self.upload_complete = upload_handler.upload_complete
        except Exception as e:
            self._log_error('File Upload', e)
        self.upload_time = (dt.datetime.now() - upload_time_start).seconds

    def _execute_task(self):
        self._input()

        if self.input_complete and self.input_data_verified and self.data_source.lower() in self.require_output:
            self._output()

        if self.output_complete and self.data_storage_type.lower() in self.require_upload:
            self._upload()

    def _update_task(self):
        if self.run_type.lower() not in ('testing', 'cycle'):
            if self.task_complete:
                self.TaskTable.update_last_run()
                if self.run_requested.lower() == 'true':
                    self.TaskTable.update_run_requested()
                try:
                    self.metrics = TaskMetrics(self)
                    self.metrics.submit_task_time()
                except Exception as e:
                    self._log_error('Task Update', e)

            else:
                # If dependents failed to run and task is not already paused or disabled.  Log Dependency failure
                if not self.dependents_run \
                        and (not self.Logger.disabled
                             or not self.Logger.paused):
                    self.current_action = 'Dependency Check'
                    self.current_function = 'run'
                    self._log_error('Task Update', 'Dependent Task(s) did not run')

    def _run_eval(self):
        self.ready = False
        status_run = self.operational.lower() in self.run_statuses
        if self.run_type.lower() != 'cycle' and status_run:
            if self.Logger.paused and not self.Logger.disabled:
                recurrence_test = recur_test_v2(self.last_attempt, 'Hourly')
            elif self.Logger.disabled:
                recurrence_test = recur_test_v2(self.last_attempt, 'Daily')
            else:
                recurrence_test = recur_test_v2(self.last_run,
                                                self.auto_recurrence,
                                                hour=self.recurrence_hour if self.recurrence_hour else 0,
                                                day=self.auto_recurrence_day if self.auto_recurrence_day else 'Monday',
                                                day_of_month=self.recurrence_day_of_month)

            run_requested = self.run_requested.lower() == 'true'

            if recurrence_test:
                self.dependents_run = self._dependency_check()
                if run_requested:
                    # Update the latest attempt to run the task
                    self.TaskTable.update_last_attempt()
                    self.last_attempt_update = True
                    self.TaskTable.update_run_requested()
                    self.run_requested = 'FALSE'
                    run_requested = False

            self.ready = all([self.dependents_run, recurrence_test, status_run]) or run_requested
        if self.run_type.lower() in ('testing', 'cycle'):
            self.ready = True

    def set_to_testing(self):
        self.run_type = 'Testing'

    def create_dynamic_name(self):
        """
        Create a dynamic file name
        """
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

    def run_task(self):
        """
        Run Task
        Reads task instructions from the database
        Evaluates new instructions
        Runs the task if needed
        """
        try:
            # Check to see if Task should be run and sets ready to True or False
            self._run_eval()

            if self.ready:
                try:
                    # Print task startup information
                    with self.print_lock:
                        self.TaskConsole.task_startup()

                    if not self.last_attempt_update:
                        # Update the latest attempt to run the task
                        self.TaskTable.update_last_attempt()

                    # Run the task
                    self._execute_task()

                    # Verify the task has been completed
                    self._verify_task_complete()

                    # Update all necessary task info in Database
                    self._update_task()

                    # Print task shutdown information
                    with self.print_lock:
                        self.TaskConsole.task_shutdown()
                except Exception as e:
                    check = e
                finally:
                    if self.last_run != self.MetaData.last_run \
                            or self.last_attempt != self.MetaData.last_attempt:
                        self.TaskTable.update_meta_data()
            # Check if paused task should be be set back to Operational
            self.TaskTable.resume_task()

        except Exception as e:
            with self.print_lock:
                print('Exception on {}:'.format(self.name), str(e))
            self._log_error('Run Task', str(e))
            raise e

        finally:
            if self.run_type.lower() == 'testing':
                self.TaskConsole.print_test_results()
