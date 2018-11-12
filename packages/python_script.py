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
        if task.storage_type == 'project':
            self.file_path = os.path.join(task.main_dir, 'project_automation', self.file_path)
        elif task.storage_type == 'script':
            self.file_path = os.path.join(task.main_dir, 'script_storage')
        self.script_path = os.path.join(self.file_path, self.file_name)
        self.successful_run = False
        self.stream_data = None
        self.rc = None

    def run_script(self):
        """
        Call python script and read Return Codes
        """
        try:
            # Generate command console python command
            command = 'python' + ' "' + self.script_path + '"'
            os.chdir(os.path.dirname(self.script_path))
            child = sp.Popen(command)
            communicate = child.communicate()
            self.stream_data = child.communicate()[0]

            # rc is the return code of the script
            self.rc = child.returncode
            os.chdir(self.task.main_dir)
            # Check return code
            print(self.task.name, 'Return Code: {}'.format(self.rc))
            print('Stream Data: {}'.format(str(self.stream_data)))
            if self.rc == 0:
                self.successful_run = True

        except Exception as e:
            raise e