import json
import datetime as dt
from automator import PriorityOrganizer
from queue import Queue
from models import Task, SnowFlakeDW, SnowflakeConsole
from collections import namedtuple
from automator_utilities import find_main_dir


class TaskTester:

    def __init__(self, database_connection, test_only=True):
        """
        Automator Settings
        """
        self.test_only = test_only
        # Database Object to use
        self.db = database_connection
        self.dw = SnowflakeConsole(self.db)
        # Schema and Table containing task instructions
        self.task_table = 'D_POST_INSTALL.T_AUTO_TASKS'

        self.main_dir = find_main_dir(__file__)

        self.task_table_column_names = []
        self.task_table_data = []

        # All Task Objects Store by Task ID
        self.task_objects = {}

        self.TaskData = None

        # Queue for running tasks
        self.run_queue = Queue()

        # Separated Task Lists
        self.python_tasks = []
        self.command_tasks = []
        self.query_tasks = []

    def _clean_task_data_header(self):
        for i, item in enumerate(self.task_table_column_names):
            if item.lower()[-1] == 'x':
                self.task_table_column_names[i] = item.lower().replace('x', '')
            else:
                self.task_table_column_names[i] = item.lower()

    def get_tasks(self):
        self.dw.get_table_data(self.task_table)
        self.task_table_column_names = self.dw.query_columns
        self._clean_task_data_header()
        self.TaskData = namedtuple('TaskData', ' '.join(self.task_table_column_names))
        self.task_table_data = self.dw.query_results

    def create_task_objects(self, testing):
        """
        Add tasks to Task Object Dictionary
        """
        #  Review all data in table and create the task_object dict
        for task in self.task_table_data:

            new_task = Task(self.TaskData._make(task), working_dir=self.main_dir, run_type=testing)
            #  If the task id doesn't exist in the dict then create it
            if new_task.id not in self.task_objects.keys():
                self.task_objects[new_task.id] = new_task

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

    def check_priorities(self):

        missing_priority = False
        for task in self.task_objects.values():
            if not getattr(task, 'priority', None):
                missing_priority = True
                break
        if missing_priority:
            priorities = PriorityOrganizer()
            priorities.find_priorities()
            for task in self.task_objects.values():
                priority = priorities.priority_queue.get(int(task.id))
                setattr(task, 'priority', priority)

    def setup_queue(self, list):
        for task in list:
            self.run_queue.put(task)

    def run_items_in_queue(self):
        while not self.run_queue.empty():
            task = self.run_queue.get()[1]
            task.run_task()
            self.run_queue.task_done()

    def run_specific_task(self, task_id):
        self.get_tasks()
        testing = 'Testing' if self.test_only else 'Automated'
        self.create_task_objects(testing)
        self.check_priorities()
        task = self.task_objects[task_id]
        task.run_task()

    def run(self):
        self.get_tasks()
        testing = 'Testing' if self.test_only else 'Automated'
        self.create_task_objects(testing)
        self.check_priorities()
        self.organize_tasks()
        self.setup_queue(self.python_tasks)
        self.run_items_in_queue()
        self.setup_queue(self.command_tasks)
        self.run_items_in_queue()
        self.setup_queue(self.query_tasks)
        self.run_items_in_queue()


if __name__ == '__main__':
    """
    Use this for testing tasks
    Set the specific task id for running any task in test mode
    """
    run_all_tasks_marked_for_testing = False

    db = SnowFlakeDW()
    db.set_user('JDLAURET')
    db.open_connection()
    specific_task_id = 758
    run_as_test = True
    app = TaskTester(db)
    try:
        if run_all_tasks_marked_for_testing:
            app.run()
        elif specific_task_id:
            if not run_as_test:
                app.test_only = False
            app.run_specific_task(specific_task_id)
        else:
            app.test_only = False
            app.run()
    except Exception as e:
        print(e)
        raise e
    finally:
        db.close_connection()
