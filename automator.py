import datetime as dt
import os
import shutil
import time
from queue import Queue
from threading import Thread

from automator_utilities import find_main_dir
from models import SnowflakeConsole, SnowFlakeDW, Task


class TaskRunner(Thread):
    """Thread executing tasks from a given tasks queue"""

    def __init__(self, queue):
        super(TaskRunner, self).__init__()
        self._q = queue
        self.daemon = True
        self.start()

    def run(self):
        while True:
            if not self._q.empty():
                task = self._q.get()
                try:
                    task.run_task()
                except Exception as e:
                    print(e)
                    raise e
                finally:
                    self._q.task_done()


class TaskThreadPool:
    """ Pool of threads consuming tasks for a queue """

    def __init__(self, num_threads):
        self._q = Queue()
        for _ in range(num_threads):
            TaskRunner(self._q)

    def add_task(self, task):
        """ Add a tasks to the queue """
        self._q.put(task)

    def wait_completion(self):
        """ Wait for completion of all the tasks in the queue """
        self._q.join()


class PriorityOrganizer:
    def __init__(self):
        self.task_list = []
        self.task_dict = {}
        self.priority_queue = {}

    def _get_task_list(self):
        query = '''
        SELECT ID, DEPENDENCIES
        FROM D_POST_INSTALL.T_AUTO_TASKS
        '''
        db = SnowFlakeDW()
        db.set_user('JDLAURET')

        dw = SnowflakeConsole(db)

        db.open_connection()
        try:
            dw.execute_query(query)
        finally:
            db.close_connection()

        self.task_list = dw.query_results

    def _create_task_dict(self):
        for row in self.task_list:
            id, dependents = row[0], row[1]
            if dependents:
                dependents = dependents.split(',')

            self.task_dict[id] = {
                'id': id,
                'dependents': dependents,
            }

    def _zero_priorities(self):
        for key in self.task_dict.keys():
            task_data = self.task_dict[key]
            dependents = task_data.get('dependents')
            if not dependents:
                self.priority_queue[task_data.get('id')] = 0

    def _set_priority(self):
        for i in range(2):
            for key in self.task_dict.keys():
                task_data = self.task_dict[key]
                dependents = task_data.get('dependents')
                if dependents:
                    for dependent in dependents:
                        dependent_id = int(dependent)
                        task_id = task_data.get('id')
                        if not self.priority_queue.get(task_id):
                            self.priority_queue[task_id] = self.priority_queue[dependent_id] + 1
                        elif self.priority_queue.get(task_id) <= self.priority_queue[dependent_id]:
                            self.priority_queue[task_id] = self.priority_queue[dependent_id] + 1

    def find_priorities(self):
        self._get_task_list()
        self._create_task_dict()
        self._zero_priorities()
        self._set_priority()


class Automator:
    def __init__(self):
        """
        Automator Settings
        """
        # Database Object to use
        self.db = SnowFlakeDW()
        self.db.set_user('JDLAURET')
        self.db.set_schema('D_POST_INSTALL')
        self.dw = None
        # Schema and Table containing task instructions
        self.task_table = 'T_AUTO_TASKS'
        self.meta_data_table = 'T_AUTO_META_DATA'

        self.task_table_column_names = []
        self.task_table_data = []

        # All Task Objects Store by Task ID
        self.task_objects = {}

        # Separated Task Lists
        self.priority_queues = {}

        self.meta_data = {}

        # Max number of threads to have running
        self.max_task_num_threads = 7
        self.task_pool = TaskThreadPool(self.max_task_num_threads)

    def set_database(self, new_db):
        """
        Change Database Object
        :param new_db: New DB Object
        """
        self.db = new_db

    def set_database_table(self, table_name):
        """
        Change Task Table Name
        :param table_name: New Table Name including Schema
        """
        self.task_table = table_name

    def _get_meta_data(self):
        query = 'SELECT * FROM {table}'.format(table=self.meta_data_table)
        self.dw.execute_query(query)
        header = self.dw.query_columns
        results = self.dw.query_results
        for i, col in enumerate(results[0]):
            self.meta_data[header[i].lower()] = col

    def _status_running(self):
        query = 'UPDATE {table} SET CURRENT_STATUS = \'Running\''.format(table=self.meta_data_table)
        self.dw.execute_sql_command(query)

    def _status_sleeping(self):
        query = 'UPDATE {table} SET CURRENT_STATUS = \'Sleeping\''.format(table=self.meta_data_table)
        self.dw.execute_sql_command(query)

    def _update_last_run(self):
        query = 'UPDATE {table} SET LAST_RUN = current_timestamp::timestamp_ntz'.format(table=self.meta_data_table)
        self.dw.execute_sql_command(query)

    def _update_last_backup(self):
        query = 'UPDATE {table} SET LAST_BACKUP = current_timestamp::timestamp_ntz'.format(table=self.meta_data_table)
        self.dw.execute_sql_command(query)

    def _update_last_priority_update(self):
        query = 'UPDATE {table} SET LAST_PRIORITY_UPDATE = current_timestamp::timestamp_ntz'\
            .format(table=self.meta_data_table)
        self.dw.execute_sql_command(query)

    def backup_files(self):
        """
        Move backup files through day files
        """
        settings_file = 'settings.json'
        date_format = "%m/%d/%Y"
        today = dt.datetime.today()
        backup_dir = os.path.join(os.getcwd(), 'file_backups')
        storage_dir = os.path.join(os.getcwd(), 'file_storage')

        #  Get the last run date
        last_backup = self.meta_data.get('last_backup')

        #  Create random date if date it empty
        if not last_backup:
            last_backup = today - dt.timedelta(days=1)

        #  If last run was before today run through backup process
        if last_backup.date() != today.date() \
                and last_backup.date() < today.date():
            #  Create all file paths
            day_1 = os.path.join(backup_dir, '1 - Day 1')
            day_2 = os.path.join(backup_dir, '2 - Day 2')
            day_3 = os.path.join(backup_dir, '3 - Day 3')
            day_4 = os.path.join(backup_dir, '4 - Day 4')
            day_5 = os.path.join(backup_dir, '5 - Day 5')

            #  Delete all items in Day 5
            if os.listdir(day_5):
                for file in os.listdir(day_5):
                    file_path = os.path.join(day_5, file)
                    try:
                        if os.path.isfile(file_path):
                            os.unlink(file_path)
                    except Exception as e:
                        pass

            #  Move Day 4 items to Day 5
            if os.listdir(day_4):
                for file in os.listdir(day_4):
                    if file != 'desktop.ini':
                        source = os.path.join(day_4, file)
                        shutil.move(source, day_5)

            #  Move Day 3 items to Day 4
            if os.listdir(day_3):
                for file in os.listdir(day_3):
                    if file != 'desktop.ini':
                        source = os.path.join(day_3, file)
                        shutil.move(source, day_4)

            #  Move Day 2 items to Day 3
            if os.listdir(day_2):
                for file in os.listdir(day_2):
                    if file != 'desktop.ini':
                        source = os.path.join(day_2, file)
                        shutil.move(source, day_3)

            #  Move Day 1 items to Day 2
            if os.listdir(day_1):
                for file in os.listdir(day_1):
                    if file != 'desktop.ini':
                        source = os.path.join(day_1, file)
                        shutil.move(source, day_2)

            #  Move Stored items to Day 1
            if os.listdir(storage_dir):
                for file in os.listdir(storage_dir):
                    if file != 'desktop.ini':
                        source = os.path.join(storage_dir, file)
                        shutil.move(source, day_1)

            # Update last run date
            self._update_last_backup()

    def refresh_task_data(self):
        """
        Get Task Data from Database
        """
        self.dw.get_table_data(self.task_table)
        self.task_table_column_names = self.dw.column_names
        self.task_table_data = self.dw.query_results

    def create_task_objects(self):
        """
        Add tasks to Task Object Dictionary
        """
        main_dir = find_main_dir(__file__)
        #  Review all data in table and create the task_object dict
        for task in self.task_table_data:
            new_task = Task(task, self.task_table_column_names, working_dir=main_dir)
            #  If the task id doesn't exist in the dict then create it
            if new_task.id not in self.task_objects.keys():
                self.task_objects[new_task.id] = new_task

    def check_priorities(self):
        today = dt.datetime.today()
        last_priority_update = self.meta_data.get('last_priority_update')

        if not last_priority_update:
            last_priority_update = today - dt.timedelta(hours=1)
        hours_since_last_update = int((today - last_priority_update).seconds/60/60)

        missing_priority = False
        for task in self.task_objects.values():
            if not getattr(task, 'priority', None):
                missing_priority = True
                break
        if missing_priority or hours_since_last_update >= 1:
            priorities = PriorityOrganizer()
            priorities.find_priorities()
            for task in self.task_objects.values():
                priority = priorities.priority_queue.get(int(task.id))
                setattr(task, 'priority', priority)
            self._update_last_priority_update()

    def organize_tasks(self):
        """
        Sort tasks into 3 list
        Python Task, SQL Command Tasks, and Query Tasks
        """
        for task in self.task_objects.values():
            if task.data_source is not None:
                priority = str(task.priority)
                if priority not in self.priority_queues.keys():
                    self.priority_queues[priority] = []
                priority_queue = self.priority_queues[priority]
                if task not in priority_queue:
                    priority_queue.append(task)

    def run_task_queue(self):
        """
        Loop through queue lists and add tasks to queue
        """
        for queue_number in self.priority_queues.keys():
            queue = self.priority_queues[queue_number]
            for task in queue:
                self.task_pool.add_task(task)
            self.task_pool.wait_completion()

    def loop_sleep(self):
        """
        Sleep until the start of the next 5 minute interval
        """
        now = dt.datetime.now()
        minutes_to_sleep = 5 - now.minute % 5
        print('Automator 3 Restarting in {} minutes'.format(minutes_to_sleep))
        self._status_sleeping()
        time.sleep((minutes_to_sleep * 60) - now.second)
        now = dt.datetime.now()
        print('Automator 3 Restarting {}'.format(now))

    def run_automator(self):
        # Start Program Loop
        while True:
            try:
                self.db.open_connection()
                self.dw = SnowflakeConsole(self.db)
                #  Get meta data from T_AUTO_SAVE_DATA
                self._get_meta_data()
                #  Update Meta Data Status
                self._status_running()
                #  Backup Local Files
                self.backup_files()
                #  Get Task Data
                self.refresh_task_data()
                #  Create Task Objects
                self.create_task_objects()
                #  Create Task Priorities
                self.check_priorities()
                #  Sort Tasks into Lists
                self.organize_tasks()
                #  Place Python Tasks into the Queue
                print('Evaluating Tasks')
                self.run_task_queue()
                self._update_last_run()
                #  Sleep till next 5 minute interval 12:00, 12:05, etc
                self.loop_sleep()

            finally:
                self.db.close_connection()


if __name__ == '__main__':
    # Start Automator
    print('Automator 3 Starting')
    app = Automator()
    app.run_automator()
