from BI.utilities.email import Email

from . import *


# %% Logger
class Logger:
    def __init__(self, task):
        """
        Logging Class for Tasks
        :param task: task object
        """
        self.task = task
        self.dw = self.task.dw
        self.id = str(self.task.id)
        self.owner_email = self.task.owner_email
        self.operational = self.task.operational.lower()
        self.TaskTable = self.task.TaskTable

        self.error_table = 'D_POST_INSTALL.T_AUTO_ERROR_LOG'
        self.error_log = []

        self.datetime_format = '%Y-%m-%d %H:%M:%S'

        self.attempt_limit = 5

        self.paused = False
        self.disabled = False
        self.check_current_status()

        self.subject = 'Task {id} - {task_name} - '.format(id=self.id, task_name=self.task.name)

    def _check_error_type(self, error):
        if type(error).__name__ == 'WorksheetNotFound':
            return 'WorksheetNotFound'
        return error

    def check_current_status(self):
        """
        Looks at task.operational to set paused or disabled
        """
        if self.operational == 'paused':
            self.paused = True
        elif self.operational == 'disabled':
            self.disabled = True

    def _get_error_log(self):
        query = '''
                SELECT * FROM D_POST_INSTALL.T_AUTO_ERROR_LOG 
                WHERE TASK_ID = {id}
                ORDER BY ERROR_TIMESTAMP DESC LIMIT 10
                '''.format(id=self.id)
        self.dw.execute_query(query)
        ErrorLog = collections.namedtuple('ErrorLog', ' '.join(x.lower() for x in self.dw.column_names))
        for row in self.dw.query_results:
            error = ErrorLog._make(row)
            if error.run_type is None:
                error = error._replace(run_type='')
            self.error_log.append(error)

    def log_error(self, action, error):
        """
        Log a new error for a task and save data to settings file
        :param action: The action within the function that was being attempted
        :param error: The Exception error they was thrown
        """
        #  Log Time for error tracking
        current_error = self._check_error_type(error)
        timestamp = dt.datetime.now().strftime(self.datetime_format)
        #  Create error log object
        log_line = [[
            self.task.id,
            action,
            None,
            str(current_error),
            timestamp,
            self.task.last_attempt,
            self.task.run_type
        ]]
        #  Add object to error log
        self.dw.insert_into_table(self.error_table, log_line)
        if self.task.run_type.lower() != 'cycle':
            #  Save settings, Update Settings, Determine if status change necessary
            self.qualify_to_disable()

    def readable_error_log(self):
        """
        Create a nice human readable string from the error log
        :return: readable error log string
        """
        error_log_string = ''
        for i, error in enumerate(self.error_log):
            new_string = '''Action: {last_action}\nError: {error}\nTimestamp: {error_timestamp}\n\n''' \
                .format(last_action=str(error.last_action),
                        error=str(error.error),
                        error_timestamp=str(error.error_timestamp))
            error_log_string = error_log_string + new_string
            if i >= self.attempt_limit - 1:
                break
        return error_log_string

    def qualify_to_disable(self):
        """
        Check if the task needs to be paused or disable, after an error has been logged.
        """
        #  Get Timestamps for comparisons
        now = dt.datetime.now()
        current_date = now.date()
        #  Get all failed attempts
        self._get_error_log()
        failed_attempts = [item.error_timestamp for item in self.error_log if item.run_type.lower() != 'testing']

        if len(failed_attempts) > 0:
            #  Get all failed attempts that occurred today
            failed_attempts_today = [x for x in failed_attempts if x.date() == current_date]
            #  If attempts limit is reached for day, disable the task
            #  Else pause the task
            attempt_limit_reached = len(failed_attempts_today) >= self.attempt_limit
            run_statuses = self.task.operational.lower() in self.task.run_statuses
            if attempt_limit_reached and not self.disabled and (run_statuses or self.paused):
                self.disabled = True
                self.TaskTable.disable_task()
                self.send_disabled_email()
            elif not self.paused:
                self.paused = True
                self.TaskTable.pause_task()

    def send_no_data_notification(self):
        subject = self.subject + 'No Data Returned From Query'

        body = '''
        This is an automated message to inform you that task {id} - {task_name} was completed, 
        but the query returned no results.  If this is an error and data should be returned, please mark the 
        NO_DATA_IS_ERROR column as 'True' in D_POST_INSTALL.T_AUTO_TASKS.  To disable this automated response, 
        please mark NO_DATA_NOTIFICATION as 'False' in the same table.  
        '''.format(id=self.task.id, task_name=self.task.name)

        self.send_email(subject, body)

    def send_disabled_email(self):
        subject = self.subject + 'Task Disabled'

        body = 'This is an automated message to inform you that task {id} - {task_name} has been disabled. This is ' \
               'due to the task encountering multiple errors when it was attempted. Following is that last set of ' \
               'errors logged. If you have questions about any of the errors or what they mean, please talk to JD.' \
               '\n{error_log}'.format(id=self.task.id,
                                      task_name=self.task.name,
                                      error_log=self.readable_error_log())

        self.send_email(subject, body)

    def send_error_email(self):
        subject = self.subject + 'Automated Task Error'
        email_params = {'task_id'}
        body = """
        This is an automated message to inform you that task {task_id} - {task_name} encountered an error. The following
        error was logged: {error}\n\nIf you have questions about any of the errors or what they mean, please
        talk to JD.""".format(task_id=self.task.id,
                              task_name=self.task.name,
                              time=dt.datetime.now().strftime('%d-%m-%y %H:%M'),
                              error=self.error_log[0].error)

        self.send_email(subject, body)

    def send_email(self, subject, body):
        """
        Send an email
        :param subject:  The subject for the email
        :param body:  The body of the email
        :return:
        """
        if self.task.run_type != 'Testing':
            email = Email([self.task.owner_email] + ['jonathan.lauret@vivintsolar.com'], subject, body)
            email.send_msg()
