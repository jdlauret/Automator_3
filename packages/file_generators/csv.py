from . import *


# %% CSV Generator
class CsvGenerator:
    """
    Generate CSV in File Storage directory
    """

    def __init__(self, data, task):
        """
        Setup for csv
        The params should be handled by the Task class automatically
        However here is the basic Dictionary Needs
        { "header": [List of items for the name of each column],
          "file_name": "name_of_file.csv", - The File name for the csv
          "file_path": "path\\to\\file, - The File Path to store the file in
          "dynamic_name": "%y-%m-%d", - This is a date format in python datetime formatting
          "after_before": "after" - If the dynamic name should show up before or after the file name
        }
        :param data: A list of lists to put in a csv
        :param params: A dictionary containing all needed information
        """

        self.data = data
        self.task = task
        self.header = self.task.input_data_header
        self.file_path = self.task.file_storage
        self.file_name = self.task.file_name

        self.successful_run = False

        # Remove the header from the data set
        # if it is included in the data set
        if self.header is None:
            self.header = data[0]
            del self.data[0]

    def create_csv(self):
        """
        Write data to csv using pandas
        """
        try:
            # Convert List of Lists to DataFrame and write it to a CSV
            pd.DataFrame(self.data, columns=self.header) \
                .to_csv(os.path.join(self.file_path, self.file_name), index=False)
            self.successful_run = True
        except:
            # TODO create Exception Handling
            raise