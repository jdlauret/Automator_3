import requests
import zipfile
import io


class DownloadSurveyType:

    def __init__(self, params):
        self.survey_id = params.get('sid')
        self.survey_name = params.get('name')
        self.data_center = params.get('data_center')
        self.api_token = params.get('api_token')
        self.file_format = 'csv'

    def get_qualtrics_data(self):
        # Setting static parameters
        request_check_progress = 0
        progress_status = "in progress"
        base_url = "https://{dc}.qualtrics.com/API/v3/responseexports/".format(dc=self.data_center)
        headers = {
            "content-type": "application/json",
            "x-api-token": self.api_token,
            }

        # Step 1: Creating Data Export
        download_request_url = base_url
        download_request_payload = '{"format":"' + self.file_format + '","surveyId":"' + self.survey_id + '"}'
        download_request_response = requests.request("POST",
                                                     download_request_url,
                                                     data=download_request_payload,
                                                     headers=headers)
        progress_id = download_request_response.json()["result"]["id"]
        print('Downloading', self.survey_name)

        # Step 2: Checking on Data Export Progress and waiting until export is ready
        while request_check_progress < 100 and progress_status is not "complete":
            request_check_url = base_url + progress_id
            request_check_response = requests.request("GET", request_check_url, headers=headers)
            request_check_progress = request_check_response.json()["result"]["percentComplete"]
            if request_check_progress > 100:
                return self.get_qualtrics_data()

        # Step 3: Downloading file
        request_download_url = base_url + progress_id + '/file'
        request_download = requests.request("GET", request_download_url, headers=headers, stream=True)

        # Step 4: Unzipping the file
        zipfile.ZipFile(io.BytesIO(request_download.content)).extractall("qualtrics_data")
