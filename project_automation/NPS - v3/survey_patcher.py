import multiprocessing as mp
import requests
import csv

NUM_CORE = 30
api_token_1 = "qA9SEIX7g54z0uWv5uB3sFm1bTaVpy7tdltEOlKE"
api_token_2 = 'anqWEcM1y1sQeSCTdbeJvbWkjbFFpt2YIbeNS6b1'


def worker(arg):
    obj, meth_name = arg[:2]
    return getattr(obj, meth_name)(*arg[2:])


class SurveyUpdater:
    data_center = 'az1'

    def __init__(self, data, header, api_token, decrement=True):
        self.data = data
        self.header = header
        if decrement:
            self.decrement = 'true'
        else:
            self.decrement = 'false'
        self.survey_id = self.data[header.index('Survey ID')]
        self.response_id = self.data[header.index('Response ID')]
        self.patch_field = ''
        self.patch_value = ''
        self.patch_fail = False
        self.delete_fail = False
        self.api_token = api_token

    def update_survey(self):
        self.patch_field = self.data[self.header.index('Patch Column')]
        self.patch_value = self.data[self.header.index('Patch Value')]
        base_url = "https://{0}.qualtrics.com/API/v3/responses/{1}".format(self.data_center, self.response_id)
        headers = {
            "content-type": "application/json",
            "x-api-token": self.api_token,
        }
        request_url = base_url
        request_payload = '{"surveyId": "' \
                          + self.survey_id \
                          + '", "embeddedData": {"' \
                          + self.patch_field \
                          + '": "' \
                          + self.patch_value \
                          + '"}}'
        response = requests.put(request_url,
                                data=request_payload,
                                headers=headers)

        if response.status_code != 200:
            print('Failed Update for:\nResponse ID: {0}\nSurvey ID: {1}\nField: {2})'
                  .format(self.response_id,
                          self.survey_id,
                          self.patch_field))
            self.data.append(response.reason)
            self.patch_fail = True
        else:
            print('Patched Survey {0}'.format(self.response_id))

    def delete_survey(self):
        base_url = "https://{0}.qualtrics.com/API/v3/responses/{1}?surveyId={2}&decrementQuotas={3}"\
            .format(self.data_center,
                    self.response_id,
                    self.survey_id,
                    self.decrement)
        headers = {
            "content-type": "application/json",
            "x-api-token": self.api_token,
        }
        request_url = base_url
        response = requests.delete(request_url, headers=headers)
        if response.status_code != 200:
            print('{2} Failed'
                  'There was an issue with deleting the response\n'
                  'The following code was issued: {0}\n'
                  '{1}'.format(response.status_code, response.reason, self.response_id))
            self.data.append(response.reason)
            self.delete_fail = True
        else:
            print('Deleted Survey {0}'.format(self.response_id))


def open_csv(file_name, folder_name):
    data_set = list()
    file = folder_name + '\\' + file_name
    with open(file, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            data_set.append(row)
    return data_set


if __name__ == "__main__":
    delete = True
    patch = True

    api_token_to_use = api_token_2
    pool = mp.Pool(NUM_CORE)

    failed_patch_items = []
    failed_delete_items = []

    if patch:
        patch_set = open_csv('Patch Data.csv', 'data')
        patch_header = patch_set[0]
        for i, item, in enumerate(patch_header):
            if 'Survey ID' in item and item != 'Survey ID':
                patch_header[i] = 'Survey ID'
        del patch_set[0]
        failed_header = patch_header.copy()
        failed_header = failed_header.append('Failure Reason')
        failed_patch_items.append(patch_header)
        patch_objects = [SurveyUpdater(line, patch_header, api_token_to_use) for line in patch_set]
        pool.map(worker, ((obj, 'update_survey') for obj in patch_objects))

    if delete:
        delete_set = open_csv('Deletion.csv', 'data')
        delete_header = delete_set[0]
        for i, item, in enumerate(delete_header):
            if 'Survey ID' in item \
                    and item != 'Survey ID':
                delete_header[i] = 'Survey ID'
        del delete_set[0]
        failed_header = delete_header.copy()
        failed_header = failed_header.append('Failure Reason')
        if 'Survey ID' in delete_header[0]:
            delete_header[0] = 'Survey ID'

        delete_objects = [SurveyUpdater(line, delete_header, api_token_to_use)for line in delete_set]
        pool.map(worker, ((obj, 'delete_survey') for obj in delete_objects))

    pool.close()
    pool.join()

    if patch:
        for obj in patch_objects:
            if obj.patch_fail:
                failed_patch_items.append(obj.data)

        if len(failed_patch_items) > 0:
            print('There were {0} patch failures, a log has been created'.format(len(failed_patch_items) - 1))
            fn = 'Failed Patch Items.csv'
            try:
                file = open(fn, 'r')
            except IOError:
                file = open(fn, 'w+')
            file.close()
            with open(fn, 'w') as csvfile:
                writer = csv.writer(csvfile, dialect='excel')
                writer.writerow(failed_patch_items)
                csvfile.close()

    if delete:
        for obj in delete_objects:
            if obj.delete_fail:
                failed_delete_items.append(obj.data)

        if len(failed_delete_items) > 0:
            print('There were {0} deletion failures, a log has been created'.format(len(failed_delete_items)-1))
            fn = 'Failed Delete Items.csv'
            try:
                file = open(fn, 'r')
            except IOError:
                file = open(fn, 'w+')
            file.close()
            with open(fn, 'w') as csvfile:
                writer = csv.writer(csvfile, dialect='excel')
                writer.writerow(failed_delete_items)
                csvfile.close()
