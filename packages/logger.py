from . import *
from BI.utilities.email import Email

# %% Logger
class Logger:
    def __init__(self, task):
        """
        Logging Class for Tasks
        :param task: task object
        """
        self.task = task
        self.id = str(self.task.id)

        self.error_table = 'D_POST_INSTALL.T_AUTO_ERROR_LOG'

        self.datetime_format = '%Y-%m-%d %H:%M:%S'

        self.attempt_limit = 5

        self.owner_email = None

        self.paused = False
        self.disabled = False
        self.check_current_status()

        self.subject = 'Task {id} - {task_name} - '.format(id=self.id, task_name=self.task.name)

    def update_task_data(self, task):
        """
        Reassign Task Object
        :param task:
        """
        self.task = task

    def check_current_status(self):
        """
        Looks at task.operational to set paused or disabled
        """
        if self.task.operational.lower() == 'paused':
            self.paused = True
        if self.task.operational.lower() == 'disabled':
            self.disabled = True

    def log_error(self, function, action, error):
        """
        Log a new error for a task and save data to settings file
        :param function: Function of task that was active when error occured
        :param action: The action within the function that was being attempted
        :param error: The Exception error they was thrown
        """
        #  Log Time for error tracking
        timestamp = dt.datetime.now().strftime(self.datetime_format)

        #  Create error log object
        log_line = [[self.task.id, action, function, str(error), timestamp, self.task.last_attempt]]
        #  Add object to error log
        self.task.dw.insert_into_table(self.error_table, log_line)
        self.task._get_error_log()
        #  Save settings, Update Settings, Determine if status change necessary
        self.qualify_to_disable()

    def readable_error_log(self):
        """
        Create a nice human readable string from the error log
        :return: readable error log string
        """
        error_log_string = ''
        for i, error in enumerate(self.task.error_log):
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
        failed_attempts = [item.error_timestamp for item in self.task.error_log]

        if len(failed_attempts) > 0:
            #  Get all failed attempts that occured today
            failed_attempts_today = [x for x in failed_attempts if x.date() == current_date]
            #  If attemps limit is reached for day, disable the task
            #  Else pause the task
            if len(failed_attempts_today) >= self.attempt_limit and not self.disabled:
                self.disabled = True
                self.task.disable_task()
                self.send_disabled_email()
            elif not self.paused:
                self.paused = True
                self.task.pause_task()

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
        body = """
        This is an automated message to inform you that task {id} - {task_name} encountered an error. The following
         error was logged: {error}\n\nIf you have questions about any of the errors or what they mean, please 
         talk to JD.""".format(task_id=self.task.id,
                               task_name=self.task.name,
                               time=dt.datetime.now().strftime('%d-%m-%y %H:%M'),
                               error=self.task.error_log[0].error)

        self.send_email(subject, body)

    def send_email(self, subject, body):
        """
        Send an email
        :param subject:  The subject for the email
        :param body:  The body of the email
        :return:
        """
        email = Email([self.task.owner_email, 'jonathan.lauret@vivintsolar.com'], subject, body)
        email.send_msg()
