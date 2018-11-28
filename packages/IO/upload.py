from . import *


class Upload:

    def __init__(self, task):
        self.task = task
        self.upload_type = self.task.storage_type
        self.upload_id = self.task.storage_id

        self.upload_complete = False

    def upload(self):
        if self.upload_type.lower() == 'google drive':
            for file in os.listdir(self.task.file_storage):
                if file == self.task.file_name:
                    try:
                        drive = GDrive()
                        file_path = self.task.file_storage
                        drive.upload_file(self.task.file_name,
                                          file_path,
                                          self.upload_id,
                                          replace_existing=True)
                        self.upload_complete = True
                    except Exception as e:
                        raise e
