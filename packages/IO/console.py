from . import *


class TaskConsole:
    """
    Task Console is simply a quick way to print things to the console for the Task object
    """

    def __init__(self, task):
        """
        Requires a Task object from packages > task.py
        :param task: Task object
        """
        self.task = task

    def task_startup(self):
        """
        Print task start up information
        """
        if self.task.run_requested.lower() == 'true':
            print('    IN PROGRESS - Manual Request -', self.task.name)

        elif self.task.run_type == 'Testing':
            print('    IN PROGRESS - Testing -', self.task.name)

        elif self.task.run_type == 'Cycle':
            print('    IN PROGRESS - Cycle -', self.task.name)
        else:
            if not self.task.last_run:
                print('    FIRST RUN')

            print('    IN PROGRESS - Automated - Priority:', self.task.priority, '-', self.task.name)

    def task_shutdown(self):
        """
        Print task shutdown information
        """
        if self.task.task_complete:
            print('    COMPLETED -', self.task.name)

        elif not self.task.task_complete and self.task.error_log:
            print('    NOT COMPLETED -', self.task.name, '- ERROR -', self.task.error_log[0].error)

        else:
            print('    NOT COMPLETED -', self.task.name)

    def print_test_results(self):
        """
        Print Test Results for task testing
        """
        print('')
        print('    TEST RESULTS')
        if not self.task.ready:
            print('    Task did not pass recurrence test')
        print('    Task Status:', self.task.operational)
        print('    Task Completed:', self.task.task_complete)
        print('    Input Type:', self.task.data_source)
        print('    Input Complete:', str(self.task.input_complete))

        if self.task.data_source.lower() in self.task.require_output:
            print('    Output Type:', self.task.data_storage_type)
            print('    Output Complete:', self.task.output_complete)
        else:
            print('    Input Type does not require an output')

        if self.task.data_storage_type is not None:
            if self.task.data_storage_type.lower() in self.task.require_upload:
                print('    Upload Complete:', self.task.upload_complete)
            else:
                print('    Output Type does not require an upload')

        for error in self.task.error_log:
            hour = (dt.datetime.now() - error.error_timestamp).seconds /60 / 60
            if hour <= 1:
                print('')
                print('    ERROR MSG:', error.error)
                print('    LAST FUNC:', error.last_function)
                print('    TIMESTAMP:', error.error_timestamp)

    def print_message(self, message):
        """
        Print a message
        :param message: the message to print
        """
        print(message)
