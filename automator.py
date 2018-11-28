__author__ = "JD Lauret"
__credits__ = ['JD Lauret', ]
__version__ = "1.0"
__maintainer__ = "JD LAURET"
__email__ = "jonathan.lauret@vivintsolar.com"
__status__ = "Production"

import csv
import datetime as dt
import os
import shutil
import time
from collections import namedtuple
from queue import Queue
from threading import Thread

from BI.data_warehouse import Snowflake, SnowflakeV2, SnowflakeConnectionHandlerV2
from packages.utilities import find_main_dir
from packages.task import Task


class TaskRunner(Thread):
    """Thread executing tasks from a given tasks queue"""

    def __init__(self, queue, thread_num):
        super(TaskRunner, self).__init__()
        self._q = queue
        self.daemon = True
        thread_name = 'Queue Thread ' + str(thread_num) + ' Started'
        self.setName(thread_name)
        print(self.getName())
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
        for i in range(num_threads):
            TaskRunner(self._q, i)

    def add_task(self, task):
        """ Add a tasks to the queue """
        self._q.put(task)

    def wait_completion(self):
        """ Wait for completion of all the tasks in the queue """
        self._q.join()


class Automator:
    def __init__(self, test_task_id=None, test_loop_count=0):
        """
        Automator Settings
        Basic start up settings, when testing a single task no Thread Pool is created.
        If Module Testing is enabled on test tasks will be run
        """
        # Specific Task testing information
        self.test_task_id = test_task_id
        self.task_test = False
        self.test_loop_count = test_loop_count
        print('Starting Automator 3')
        # Database Connection
        self.db_connection = SnowflakeConnectionHandlerV2()
        self.dw = SnowflakeV2(self.db_connection)
        self.dw.set_user('JDLAURET')

        # Default schema to work out of
        self.dw.set_schema('D_POST_INSTALL')

        # Table containing task instructions
        self.task_table = 'T_AUTO_TASKS'

        # Table containing meta data
        self.meta_data_table = 'T_AUTO_META_DATA'

        # Current working directory
        self.main_dir = find_main_dir(__file__)

        # Basic task data
        self.task_table_column_names = []
        self.task_table_data = []

        # All Task Objects Store by Task ID
        self.task_objects = {}

        # Used for named tuple defined in _refresh_task_data
        self.TaskData = None

        # Queue information
        self.priority_queues = {}
        self.number_of_queues = 0

        # Meta Data Storage
        self.meta_data = {}

        # If a test task id was provided turn task_test on
        if self.test_task_id:
            self.task_test = True

        else:
            # Max number of threads to have running
            self.max_task_num_threads = 7
            # Setup up task pool
            self.task_pool = TaskThreadPool(self.max_task_num_threads)
            print('{} threads in ThreadPool'.format(self.max_task_num_threads))

    def set_database_table(self, table_name):
        """
        Change Task Table Name
        :param table_name: New Table Name including Schema
        """
        self.task_table = table_name

    def _get_meta_data(self):
        """
        Get all data from meta data table and push it into a MetaData named tuple
        """
        query = 'SELECT * FROM {table}'.format(table=self.meta_data_table)
        self.dw.execute_query(query)
        MetaData = namedtuple('MetaData', ' '.join(x.lower() for x in self.dw.column_names))
        self.meta_data = MetaData._make(self.dw.query_results[0])

    def _status_running(self):
        """
        Change Current Status in meta data table to Running
        """
        query = 'UPDATE {table} SET CURRENT_STATUS = \'Running\''.format(table=self.meta_data_table)
        self.dw.execute_sql_command(query)

    def _status_sleeping(self):
        """
        Change Current Status in meta data table to Sleeping
        """
        query = 'UPDATE {table} SET CURRENT_STATUS = \'Sleeping\''.format(table=self.meta_data_table)
        self.dw.execute_sql_command(query)

    def _update_last_run(self):
        """
        Update Last Run to current timestamp
        """
        query = 'UPDATE {table} SET LAST_RUN = current_timestamp::timestamp_ntz'.format(table=self.meta_data_table)
        self.dw.execute_sql_command(query)

    def _update_last_backup(self):
        """
        Update Last Backup to current timestamp
        """
        query = 'UPDATE {table} SET LAST_BACKUP = current_timestamp::timestamp_ntz'.format(table=self.meta_data_table)
        self.dw.execute_sql_command(query)

    def _update_last_priority_update(self):
        """
        Update last priority update to current timestamp
        """
        query = 'UPDATE {table} SET LAST_PRIORITY_UPDATE = current_timestamp::timestamp_ntz' \
            .format(table=self.meta_data_table)
        self.dw.execute_sql_command(query)

    def _update_task_backup(self):
        """
        Update last task backup to current timestamp
        """
        query = 'UPDATE {table} SET TASK_BACKUP = current_timestamp::timestamp_ntz' \
            .format(table=self.meta_data_table)
        self.dw.execute_sql_command(query)

    def backup_files(self):
        """
        This function should only run once per day and uses meta_data.last_backup
        to determine the last time the action was performed. The actions taken to
        move files in file_backups dir are:
            Delete all files stored in Day 5.
            Then move all files in Day 4 to 5, 3 to 4, 2 to 3, 1 to 2.
            Last move all files in file_storage to Day 1
        """
        # The current time and define directories to work with
        today = dt.datetime.today()
        backup_dir = os.path.join(os.getcwd(), 'file_backups')
        storage_dir = os.path.join(os.getcwd(), 'file_storage')

        #  Get last_backup from MetaData
        last_backup = self.meta_data.last_backup

        #  If last_backup has never occurred set it to Yesterday
        if not last_backup:
            last_backup = today - dt.timedelta(days=1)

        # To execute backup process
        # Backup cannot have been performed today
        # and at least 1 day must have passed
        if last_backup.date() != today.date() \
                and last_backup.date() < today.date():
            # Create all file paths
            day_1 = os.path.join(backup_dir, '1 - Day 1')
            day_2 = os.path.join(backup_dir, '2 - Day 2')
            day_3 = os.path.join(backup_dir, '3 - Day 3')
            day_4 = os.path.join(backup_dir, '4 - Day 4')
            day_5 = os.path.join(backup_dir, '5 - Day 5')

            # Delete all files in Day 5
            if os.listdir(day_5):
                for file in os.listdir(day_5):
                    file_path = os.path.join(day_5, file)
                    try:
                        if os.path.isfile(file_path):
                            os.unlink(file_path)
                    except Exception as e:
                        pass

            # Move files from Day 4 to Day 5
            if os.listdir(day_4):
                for file in os.listdir(day_4):
                    if file != 'desktop.ini':
                        source = os.path.join(day_4, file)
                        shutil.move(source, day_5)

            # Move files from Day 3 to Day 4
            if os.listdir(day_3):
                for file in os.listdir(day_3):
                    if file != 'desktop.ini':
                        source = os.path.join(day_3, file)
                        shutil.move(source, day_4)

            # Move files from Day 2 to Day 3
            if os.listdir(day_2):
                for file in os.listdir(day_2):
                    if file != 'desktop.ini':
                        source = os.path.join(day_2, file)
                        shutil.move(source, day_3)

            # Move files from Day 1 to Day 2
            if os.listdir(day_1):
                for file in os.listdir(day_1):
                    if file != 'desktop.ini':
                        source = os.path.join(day_1, file)
                        shutil.move(source, day_2)

            # Move files from file_storage to Day 1
            if os.listdir(storage_dir):
                for file in os.listdir(storage_dir):
                    if file != 'desktop.ini':
                        source = os.path.join(storage_dir, file)
                        shutil.move(source, day_1)

            # Update last backup date
            self._update_last_backup()

    def _clean_task_data_header(self):
        """
        Change all header values to Lowercase
        and remove the X from fields like NAMEX
        """
        for i, item in enumerate(self.task_table_column_names):
            if item.lower()[-1] == 'x':
                self.task_table_column_names[i] = item.lower().replace('x', '')
            else:
                self.task_table_column_names[i] = item.lower()

    def _refresh_task_data(self):
        """
        Get all tasks from task table and create TaskData named tuple for each task
        """
        # Get all tasks from task_table
        self.dw.get_table_data(self.task_table)

        self.task_table_data = self.dw.query_results
        self.task_table_column_names = self.dw.column_names

        # adjust headers for consistency
        self._clean_task_data_header()

        # Create TaskData named tuple
        self.TaskData = namedtuple('TaskData', ' '.join(self.task_table_column_names))

        # Get meta data and perform task backup
        self._get_meta_data()
        self._backup_tasks()

    def _backup_tasks(self):
        """
        In the event that a the Task Table were dropped or items deleted
        Create a copy of the Task Table in as a CSV
        and store it in the task_backups directory.
        """

        # Directories to that will be worked in
        backup_folder = os.path.join(self.main_dir, 'task_backup')
        file_name = os.path.join(backup_folder, 'task_data.csv')

        # Check when the last task_backup was performed
        # If task_backup date doesn't exist set task_backup to 1 hour ago
        if not self.meta_data.task_backup:
            task_backup = dt.datetime.now() - dt.timedelta(hours=1)
        else:
            task_backup = self.meta_data.task_backup.replace(second=0, microsecond=0)

        # Create integer of how many hours have passed since the last backup was performed
        hours_since_backup = int((dt.datetime.now() - task_backup).seconds / 60 / 60)

        # Perform backup once per hour
        if hours_since_backup >= 1:
            print('Backing up task table')
            # Write table data to csv
            if self.dw.query_results:
                with open(file_name, mode='w') as outfile:
                    writer = csv.writer(outfile, delimiter=',', quotechar='"',
                                        quoting=csv.QUOTE_MINIMAL, lineterminator='\n')
                    for row in self.dw.query_with_header:
                        writer.writerow(row)
            # Update the tast backup timestamp
            self._update_task_backup()

    def _create_task_objects(self):
        """
        Creates a dictionary containing each task object using
        the task ID as the key and the Task object as the value
        """

        for task in self.task_table_data:
            # Create TaskData named tuple
            task_data = self.TaskData._make(task)

            # If the id key does no exist create key value pair in task_objects
            if task_data.id not in self.task_objects.keys():
                self.task_objects[task_data.id] = Task(self.TaskData._make(task),
                                                       self.db_connection,
                                                       working_dir=self.main_dir)

    def _check_priorities(self):
        """
        Priorities are used to help identify the order in which to run each task
        The priorities are determined using the task dependencies data.
        (See PriorityOrganizer class for how this is done)
        Update priorities is performed on startup, once per hour, and when a new
        Task object is created
        """
        # Missing Priority flag
        missing_priority = False

        # Get Today's date and the current hour
        today = dt.datetime.today().replace(minute=0, second=0, microsecond=0)

        # Get last_priority_update date and hour. If it does not exist, set to 1 hour ago
        if not self.meta_data.last_priority_update:
            last_priority_update = today - dt.timedelta(hours=1)
        else:
            last_priority_update = self.meta_data.last_priority_update.replace(minute=0, second=0, microsecond=0)

        hours_since_last_update = int((today - last_priority_update).seconds / 60 / 60)

        # Look at all Task object, check if priority value exists
        for task in self.task_objects.values():
            # If no priority value is set, raise missing priority flag
            if not getattr(task, 'priority', None):
                missing_priority = True
                break

        # If missing priority flag has been raised,
        # an hour or more has passed since last update,
        # or a new task has been created
        if missing_priority or hours_since_last_update >= 1:
            # Create Priority list
            priorities = PriorityOrganizer(self)
            priorities.find_priorities()

            # Set the number of queues to generate
            self.number_of_queues = priorities.number_of_queues + 1

            # Set priority on each task object
            for task in self.task_objects.values():
                priority = priorities.priority_queue.get(int(task.id))
                setattr(task, 'priority', priority)

            # Update last priority timestamp
            self._update_last_priority_update()

    def _setup_queues(self, test_only=False):
        """
        Add task objects to assigned priority queue
        :param test_only: Put Task objects into Test Mode
                          if operational Test Only
        """
        for task in self.task_objects.values():
            # Verify task has a data source
            if task.data_source is not None:
                # Get tasks assigned priority
                priority = str(task.priority)

                # Create queue if it does not exist
                if priority not in self.priority_queues.keys():
                    self.priority_queues[priority] = []

                # Get current queue
                priority_queue = self.priority_queues[priority]

                # Put task objects into queue
                if not test_only:
                    priority_queue.append(task)
                elif test_only and task.operational.lower() == 'test only':
                    task.set_to_testing()
                    priority_queue.append(task)

    def _run_task_queues(self):
        """
        Loop through each task in a queue list and add task to queue
        """
        print('Evaluating Tasks')
        for queue_number in range(self.number_of_queues):
            queue = self.priority_queues[str(queue_number)]
            if len(queue) > 0:
                for task in queue:
                    self.task_pool.add_task(task)
                self.task_pool.wait_completion()

    def _sleep(self):
        """
        Find when the next 5 minute interval. (10:00, 10:05, 10:10)
        Sleep till next 5 minute interval begins
        """
        now = dt.datetime.now()
        # How long until next run interval
        minutes_to_sleep = 5 - now.minute % 5
        print('Automator 3 Restarting in {} minutes'.format(minutes_to_sleep))
        time.sleep((minutes_to_sleep * 60) - now.second)
        now = dt.datetime.now()
        print('Automator 3 Restarting {}'.format(now))

    def test_task(self, task_id):
        """
        Get a specific task, set task to testing, then run task
        :param task_id: ID of the task to test
        """
        print('Running Automator 3 - MODE: Task Test')
        # Get requested task
        task = self.task_objects.get(int(task_id))
        print('Running Task: {id} - {name}'.format(id=task.id, name=task.name))
        # Set task to testing and run
        task.set_to_testing()
        task.run_task()

    def module_tests(self):
        """
        Run Task objects designed for testing each module.
        """
        print('Running Automator 3 - MODE: Module Testing')
        self.dw.open_connection()
        try:
            # Get Task Data
            self._refresh_task_data()

            # Create Task Objects
            self._create_task_objects()

            # Create Task Priorities
            self._check_priorities()

            # Queue up all testing tasks
            self._setup_queues(test_only=True)

            # Run module tests
            self._run_task_queues()

        except Exception as e:
            raise e

        finally:
            self.dw.close_connection()

    def run_automator(self):
        # Start Program Loop
        cycles = 0

        mode_print = False
        self.dw.open_connection()
        try:
            while True:

                cycles += 1
                print('Cycle {} Started'.format(cycles))
                try:

                    # Get Task Data
                    self._refresh_task_data()

                    # Update meta data status
                    self._status_running()

                    # Backup Local Files
                    self.backup_files()

                    # Create Task Objects
                    self._create_task_objects()

                    # Create Task Priorities
                    self._check_priorities()

                    if self.task_test:

                        # Start up requested task
                        self.test_task(self.test_task_id)
                        if not self.test_loop_count \
                                or cycles == self.test_loop_count:
                            break

                    else:
                        if not mode_print:
                            print('Running Automator 3 - MODE: Standard')
                            mode_print = True
                        # Sort Tasks into Lists
                        self._setup_queues()

                        # Setup Task queues and execute all tasks
                        self._run_task_queues()

                        # Update the last run in meta data
                        self._update_last_run()

                        # Update meta data status
                        self._status_sleeping()

                        cycles += 1
                        print('Cycle {} Completed'.format(cycles))

                        # Sleep till next 5 minute interval 12:00, 12:05, etc
                        self._sleep()

                except Exception as e:
                    raise e

        finally:
            self.dw.close_connection()



# %% PriorityOrganizer
class PriorityOrganizer:
    TaskInfo = namedtuple('TaskInfo', 'id dependents sorted')

    def __init__(self, automator):
        self.automator = automator
        self.task_list = []
        self.task_dict = {}
        self.sorted_tasks = {}
        self.priority_queue = {}
        self.number_of_queues = 0

    def _get_task_list(self):
        query = '''
        SELECT ID, DEPENDENCIES
        FROM D_POST_INSTALL.T_AUTO_TASKS
        ORDER BY ID DESC
        '''
        self.automator.dw.execute_query(query)

        self.task_list = self.automator.dw.query_results

    def _create_task_dict(self):
        for row in self.task_list:
            row.append(False)
            task_info = self.TaskInfo._make(row)
            self.task_dict[task_info.id] = task_info

    def _zero_priorities(self):
        for key in self.task_dict.keys():
            task_data = self.task_dict[key]
            dependents = task_data.dependents
            if not dependents:
                self.priority_queue[task_data.id] = 0
                self.task_dict[key] = task_data._replace(sorted=True)

    def _set_priority(self):
        while not all(x.sorted for x in self.task_dict.values()):
            for key in self.task_dict.keys():
                task_data = self.task_dict[key]
                if task_data.dependents:
                    dependents = task_data.dependents.split(',')
                    for dependent in dependents:
                        dependent_id = int(dependent)
                        task_id = task_data.id
                        if self.task_dict.get(dependent_id) and task_id not in dependents:
                            dependent_sorted = self.priority_queue.get(dependent_id)
                            if dependent_sorted is not None:
                                if not self.priority_queue.get(task_id):
                                    self.priority_queue[task_id] = self.priority_queue[dependent_id] + 1
                                    if self.priority_queue[dependent_id] + 1 > self.number_of_queues:
                                        self.number_of_queues = self.priority_queue[dependent_id] + 1
                                elif self.priority_queue.get(task_id) <= self.priority_queue[dependent_id]:
                                    self.priority_queue[task_id] = self.priority_queue[dependent_id] + 1
                                    if self.priority_queue[dependent_id] + 1 > self.number_of_queues:
                                        self.number_of_queues = self.priority_queue[dependent_id] + 1
                                complete_dependents = []
                                for dependent in dependents:
                                    dependent_id = int(dependent)
                                    complete_dependents.append(self.task_dict[dependent_id].sorted)
                                if all(x for x in complete_dependents):
                                    self.task_dict[task_id] = task_data._replace(sorted=True)

    def find_priorities(self):
        self._get_task_list()
        self._create_task_dict()
        self._zero_priorities()
        self._set_priority()


# %% Run Automator
if __name__ == '__main__':
    # Start Automator
    app = Automator()
    # app.dw.console_messages_on()
    app.run_automator()
