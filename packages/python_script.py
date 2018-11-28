from . import *
import subprocess as sp


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

        self.script_path = None
        self.successful_run = False
        self.stream_data = None
        self.rc = None

    def _create_file_path(self):
        if self.task.storage_type == 'project':
            self.file_path = os.path.join(self.task.main_dir, 'project_automation', self.file_path)

        elif self.task.storage_type == 'script':
            self.file_path = os.path.join(self.task.main_dir, 'script_storage')

        elif self.task.storage_type == 'test':
            self.file_path = os.path.join(self.task.main_dir, 'packages', 'automator_tests', 'test_files')

        self.script_path = os.path.join(self.file_path, self.file_name)


    def run_script(self):
        """
        Call python script and read Return Codes
        """
        try:
            self._create_file_path()

            # Generate command console python command
            command = 'python' + ' "' + self.script_path + '"'

            # Change directory to python file's directory
            os.chdir(os.path.dirname(self.script_path))
            child = sp.Popen(command)

            communicate = child.communicate()
            self.stream_data = child.communicate()[0]

            # Save the return code
            self.rc = child.returncode

            # Reset directory back to starting location
            os.chdir(self.task.main_dir)

            # print(self.task.name, 'Return Code: {}'.format(self.rc))
            # print('Stream Data: {}'.format(str(self.stream_data)))

            # Check return code
            if self.rc == 0:
                self.successful_run = True

        except Exception as e:
            raise e