import os
import csv
import sys
from google_bridge import GoogleDriveUploader


def print_progress(iteration, total, prefix='', suffix='', decimals=1, bar_length=100):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        bar_length  - Optional  : character length of bar (Int)
    """
    str_format = "{0:." + str(decimals) + "f}"
    percents = str_format.format(100 * (iteration / float(total)))
    filled_length = int(round(bar_length * iteration / float(total)))
    bar = '█' * filled_length + '-' * (bar_length - filled_length)

    sys.stdout.write('\r%s |%s| %s%s %s' % (prefix, bar, percents, '%', suffix)),

    if iteration == total:
        sys.stdout.write('\n')
    sys.stdout.flush()


class CsvCreator:
    """
    Creates a CSV
    """
    def __init__(self, file_name, file_path, data, folder_id):
        self.folder_id = folder_id
        self.file_name = file_name
        self.file_path = file_path
        self.data = data

    def creator(self):
        print()
        print('Creating {0}'.format(self.file_name))
        if '.csv' not in self.file_name:
            self.file_name = self.file_name + '.csv'

        fn = os.path.join(self.file_path, self.file_name)

        try:
            file = open(fn, 'r')
        except IOError:
            file = open(fn, 'w')
        file.close()

        with open(fn, "w", newline='', encoding='utf-8') as out_file:
            wtr = csv.writer(out_file, dialect='excel')
            print_progress(0, len(self.data), prefix='Progress', suffix='Complete')
            for i, row in enumerate(self.data):
                wtr.writerow(row)
                print_progress(i+1, len(self.data), prefix='Progress', suffix='Complete')
        out_file.close()
        self.upload()

    def upload(self):
        GoogleDriveUploader(self.file_name, self.file_path, self.folder_id)
