import os
import csv


class CsvCreator:
    """
    Creates a CSV
    """
    fmt = '%H:%M:%S'
    start_time = ''
    end_time = ''
    file_name = "No File Name"
    file_path = "No File Path"
    data = "No Data"
    folder_id = ''

    def __init__(self, file_name, before_after, data, storage):

        self.folder_id = storage['folder']
        self.storage_type = storage['type']
        self.replace = storage['replace_existing']
        self.storage = storage

    def creator(self):

        if '.csv' not in self.file_name:
            self.file_name = self.file_name + '.csv'

        fn = os.path.join(self.file_path, self.file_name)

        try:
            file = open(fn, 'r')
        except IOError:
            file = open(fn, 'w')
        file.close()

        with open(fn, "w", newline='', encoding='utf-8') as out_file:
            wtr = csv.writer(out_file, delimiter=',')
            for row in self.data:
                wtr.writerow(row)
        out_file.close()
