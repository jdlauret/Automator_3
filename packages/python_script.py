from . import *
import threading
import subprocess as sp
import time


# %% Python Script
class PythonScript:
    """
    PythonScript used to run additional python scripts
    """

    def __init__(self, task):
        """
        :param task: The Task object
        """
        self.task = task
        self.file_name = task.file_name
        self.file_path = task.data_source_id

        self.timeout = 1200
        self.script_path = None
        self.successful_run = False
        self.stream_data = None
        self.rc = None
        self.command = None
        self._create_file_path()
        self._get_timeout()

    def _create_file_path(self):
        if self.task.storage_type == 'project':
            self.file_path = os.path.join(self.task.main_dir, 'project_automation', self.file_path)

        elif self.task.storage_type == 'script':
            self.file_path = os.path.join(self.task.main_dir, 'script_storage')

        elif self.task.storage_type == 'test':
            self.file_path = os.path.join(self.task.main_dir, 'packages', 'automator_tests', 'test_files')

        self.script_path = os.path.join(self.file_path, self.file_name)

        # Generate command console python command
        self.command = 'python' + ' "' + self.script_path + '"'

    def _get_timeout(self):
        query = """
        SELECT MEDIAN(NVL(DATA_RETRIEVAL_TIME, 0) + NVL(DATA_STORAGE_TIME, 0) + NVL(FILE_STORAGE_TIME, 0))
        FROM D_POST_INSTALL.T_AUTO_METRICS
        WHERE TASK_ID = {id}""".format(id=self.task.id)
        dw = self.task.dw
        dw.execute_query(query)
        result = dw.query_results
        if result:
            result = int(result[0][0])
            if result * 2 > self.timeout:
                self.timeout = (result * 2)

    def _after_timeout(self):
        print("KILL MAIN THREAD: {}".format(threading.currentThread().ident))
        raise SystemExit

    def run_script(self):
        start = time.time()
        # print('Started')
        # print('Timeout:', self.timeout)
        t = threading.Thread(target=self._execute_script)
        t.daemon = True
        t.start()
        threading.Timer(self.timeout, self._after_timeout)
        t.join()
        end = time.time()
        # print('Ended after:', str(round(end-start)), 'seconds')

    def _execute_script(self):
        """
        Call python script and read Return Codes
        """
        try:
            # Change directory to python file's directory
            os.chdir(os.path.dirname(self.script_path))
            child = sp.Popen(self.command)

            communicate = child.communicate()
            self.stream_data = child.communicate()[0]

            # Save the return code
            self.rc = child.returncode

            # Check return code
            if self.rc == 0:
                self.successful_run = True

        except Exception as e:
            raise e
        finally:
            # Reset directory back to starting location
            os.chdir(self.task.main_dir)
