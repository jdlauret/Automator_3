import json
from queue import Queue
from models import Task, SnowFlakeDW, SnowflakeConsole
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
        self.task_table_column_names = self.dw.query_columns
        self.task_table_data = self.dw.query_results

    def setup_task_dict(self, run_type):
        main_dir = find_main_dir(__file__)
        for row in self.task_table_data:
            task = Task(row, self.task_table_column_names, self.db, run_type=run_type, working_dir=main_dir)
            if task.id not in self.task_objects.keys():
                self.task_objects[task.id] = task

    def create_settings_file(self):
        settings_file = 'settings.json'
        try:
            file = open(settings_file, 'r')
        except IOError:
            file = open(settings_file, 'w')
        file.close()

        with open(settings_file) as infile:
            try:
                settings = json.load(infile)
            except:
                settings = {}

        if 'storage_file_backup' not in settings.keys():
            settings['storage_file_backup'] = {}

        with open(settings_file, 'w') as outfile:
            json.dumps(settings, outfile, indent=4, sort_keys=True)

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
        while not self.run_queue.empty():
            task = self.run_queue.get()
            task.run_task()
            self.run_queue.task_done()

    def run_specific_task(self, task_id):
        self.get_tasks()
        testing = 'Testing' if self.test_only else 'Automated'
        self.setup_task_dict(testing)
        task = self.task_objects[task_id]
        task.run_task()

    def run(self):
        self.get_tasks()
        testing = 'Testing' if self.test_only else 'Automated'
        self.create_settings_file()
        self.setup_task_dict(testing)
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
    specific_task_id = 511
    run_as_test = False
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
