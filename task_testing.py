from queue import Queue
from models import DataWarehouse, Task


class TaskTester:

    def __init__(self, test_only=True):
        """
        Automator Settings
        """
        self.test_only = test_only
        # Database Object to use
        self.dw = DataWarehouse('admin')
        # Schema and Table containing task instructions
        self.task_table = 'JDLAURET.T_AUTO_TASKS'

        self.task_table_column_names = []
        self.task_table_data = []

        # All Task Objects Store by Task ID
        self.task_objects = {}

        # Queue for running tasks
        self.run_queue = Queue()

        # Separated Task Lists
        self.python_tasks = []
        self.command_tasks = []
        self.query_tasks = []

    def get_tasks(self):
        self.dw.get_table_data(self.task_table)
        self.task_table_column_names = self.dw.column_names
        self.task_table_data = self.dw.results

    def setup_task_dict(self):
        for row in self.task_table_data:
            task = Task(row, self.task_table_column_names, run_type='Testing')
            if task.task_id not in self.task_objects.keys():
                self.task_objects[task.task_id] = task

    def organize_tasks(self):
        if self.test_only:
            for key, task in self.task_objects.items():
                if task.operational.lower() == 'testing':
                    if task.data_source is not None:
                        if task.data_source.lower() == 'python':
                            self.python_tasks.append(task)
                        if task.data_source.lower() == 'sql command':
                            self.command_tasks.append(task)
                        if task.data_source.lower() == 'sql':
                            self.query_tasks.append(task)
        else:
            for key, task in self.task_objects.items():
                if task.data_source is not None:
                    if task.data_source.lower() == 'python':
                        self.python_tasks.append(task)
                    if task.data_source.lower() == 'sql command':
                        self.command_tasks.append(task)
                    if task.data_source.lower() == 'sql':
                        self.query_tasks.append(task)

    def setup_queue(self, list):
        for task in list:
            self.run_queue.put(task)

    def run_items_in_queue(self):
        while self.run_queue.not_empty:
            task = self.run_queue.get()
            task.run_task()
            self.run_queue.task_done()

    def run_specific_task(self, task_id):
        self.get_tasks()
        self.setup_task_dict()
        task = self.task_objects[task_id]
        task.run_task()

    def run(self):
        self.get_tasks()
        self.setup_task_dict()
        self.setup_queue(self.python_tasks)
        self.setup_queue(self.command_tasks)
        self.setup_queue(self.query_tasks)


if __name__ == '__main__':
    """
    Use this for testing tasks
    Set the specific task id for running any task in test mode
    """
    run_all_tasks_marked_for_testing = True

    specific_task_id = None

    app = TaskTester()

    if run_all_tasks_marked_for_testing:
        app.run()
    elif not specific_task_id:
        app.run_specific_task(specific_task_id)
    else:
        app.test_only = False
        app.run()
