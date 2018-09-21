import json
import os
from watson_developer_cloud import NaturalLanguageUnderstandingV1
from watson_developer_cloud import WatsonException, WatsonInvalidArgument
import watson_developer_cloud.natural_language_understanding.features.v1 \
  as Features
import sys


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


def find_main_dir():
    if getattr(sys, 'frozen', False):
        # The application is frozen
        return os.path.dirname(sys.executable)
    else:
        # The application is not frozen
        # Change this bit to match where you store your data files:
        return os.path.dirname(__file__)


def get_file(file_name, folder_name):
    """
        Returns file path string
    """
    return os.path.join(find_main_dir(), folder_name + '/' + file_name)


# Watson NLU API credentials
natural_language_understanding = NaturalLanguageUnderstandingV1(
  username="a7cc117c-1f24-415a-8f4e-cf50e2c76ba7",
  password="WVIywQIg3Xtu",
  version="2017-02-27")

watson_data_file = 'watson_comment_data.json'
watson_reviewed_file = 'watson_reviewed_surveys.json'

# Opens json storage files
with open(get_file(watson_data_file, 'data')) as watson_file,\
        open(get_file(watson_reviewed_file, 'data')) as reviewed_file:
    watson_data = json.load(watson_file)
    reviewed_data = json.load(reviewed_file)

query_count = reviewed_data['metrics']['query_count']


def save_json(file, obj):
    """
        Saves an obj to a json
        :param file: name of the file to dump the data into
        :param obj: the object to dump
    """
    with open(file, 'w') as outfile:
        json.dump(obj, outfile, indent=4, sort_keys=True)


def watson_comment_analysis(comment):
    """
    Creates json keys if they don't already exist
    Submits comment to Watson NLU
    Returns the json reponse
    :param comment:
    :return:
    """
    global query_count
    if 'metrics' not in reviewed_data.keys():
        reviewed_data['metrics'] = {}
    if 'query_count' not in reviewed_data['metrics'].keys():
        reviewed_data['metrics']['query_count'] = 0
    if 'error_log' not in reviewed_data['metrics'].keys():
        reviewed_data['metrics']['error_log'] = []
    if len(comment.split(' ')) > 3:
        try:
            response = natural_language_understanding.analyze(
              text=comment,
              features=[
                  Features.Emotion(),
                  Features.Sentiment()
              ]
            )
            query_count += 1
            return response
        except WatsonException as e:
            reviewed_data['metrics']['error_log'].append([comment, str(e)])
            query_count += 1
            return 'Error'


def watson_handler(comment, survey_id, survey_type):
    """
    Checks to see if Watson analysis has already been completed
    :param comment:
    :param survey_id:
    :return:
    """
    survey_id_list = reviewed_data['survey_ids']
    if survey_id not in survey_id_list:
        reviewed_data['survey_ids'][survey_id] = {'NPS': False,
                                                  'ANPS': False}
    if reviewed_data['survey_ids'][survey_id][survey_type] is False:
        reviewed_data['survey_ids'][survey_id][survey_type] = True
        return watson_comment_analysis(comment)
    else:
        return 'Already Analyzed'


def watson_data_storage(data, data_header):
    """
    Stores and organizes Watson json data
    :param data:
    :param data_header:
    :return:
    """

    survey_id_col = data_header.index('SURVEY_ID')
    nps_comment_col = data_header.index('NPS_COMMENTS')
    anps_comment_col = data_header.index('ANPS_COMMENTS')

    if data[0] == data_header:
        del data[0]
    lines = len(data)
    print("Now building data from {0} comments".format(lines))
    if 'analysis' not in watson_data.keys():
        watson_data['analysis'] = [
            [
                'Survey ID',
                'NPS Sentiment',
                'NPS Sentiment Score',
                'NPS Joy',
                'NPS Anger',
                'NPS Sadness',
                'NPS Disgust',
                'NPS Fear',
                'ANPS Sentiment',
                'ANPS Sentiment Score',
                'ANPS Joy',
                'ANPS Anger',
                'ANPS Sadness',
                'ANPS Disgust',
                'ANPS Fear',
            ]
        ]
    print_progress(0, 30000-reviewed_data['metrics']['query_count'], prefix='Analyzing', suffix='Complete')
    global query_count
    for j, line in enumerate(data):
        if query_count >= 30000:
            break
        survey_id = line[survey_id_col]
        nps_comment = line[nps_comment_col]
        anps_comment = line[anps_comment_col]

        if nps_comment is not None:
            nps_comment = str(nps_comment)
            if 'survey_ids' not in reviewed_data.keys():
                reviewed_data['survey_ids'] = {}
            watson_comment = watson_handler(nps_comment, survey_id, 'NPS')

            if watson_comment != 'Already Analyzed' \
                    and watson_comment != 'Error' \
                    and watson_comment is not None:
                try:
                    nps_comment_sentiment = watson_comment['sentiment']['document']['label']
                except:
                    nps_comment_sentiment = None
                try:
                    nps_sentiment_score = watson_comment['sentiment']['document']['score']
                except:
                    nps_sentiment_score = None
                try:
                    nps_joy = watson_comment['emotion']['document']['emotion']['joy']
                except:
                    nps_joy = None
                try:
                    nps_anger = watson_comment['emotion']['document']['emotion']['anger']
                except:
                    nps_anger = None
                try:
                    nps_disgust = watson_comment['emotion']['document']['emotion']['disgust']
                except:
                    nps_disgust = None
                try:
                    nps_sadness = watson_comment['emotion']['document']['emotion']['sadness']
                except:
                    nps_sadness = None
                try:
                    nps_fear = watson_comment['emotion']['document']['emotion']['fear']
                except:
                    nps_fear = None
            else:
                nps_comment_sentiment = None
                nps_sentiment_score = None
                nps_joy = None
                nps_anger = None
                nps_disgust = None
                nps_sadness = None
                nps_fear = None
        else:
            nps_comment_sentiment = None
            nps_sentiment_score = None
            nps_joy = None
            nps_anger = None
            nps_disgust = None
            nps_sadness = None
            nps_fear = None

        if anps_comment is not None:
            anps_comment = str(anps_comment)
            if 'survey_ids' not in reviewed_data.keys():
                reviewed_data['survey_ids'] = []
            watson_comment = watson_handler(anps_comment, survey_id, 'ANPS')

            if watson_comment != 'Already Analyzed' \
                    and watson_comment != 'Error' \
                    and watson_comment is not None:
                try:
                    anps_comment_sentiment = watson_comment['sentiment']['document']['label']
                except:
                    anps_comment_sentiment = None
                try:
                    anps_sentiment_score = watson_comment['sentiment']['document']['score']
                except:
                    anps_sentiment_score = None
                try:
                    anps_joy = watson_comment['emotion']['document']['emotion']['joy']
                except:
                    anps_joy = None
                try:
                    anps_anger = watson_comment['emotion']['document']['emotion']['anger']
                except:
                    anps_anger = None
                try:
                    anps_disgust = watson_comment['emotion']['document']['emotion']['disgust']
                except:
                    anps_disgust = None
                try:
                    anps_sadness = watson_comment['emotion']['document']['emotion']['sadness']
                except:
                    anps_sadness = None
                try:
                    anps_fear = watson_comment['emotion']['document']['emotion']['fear']
                except:
                    anps_fear = None
            else:
                anps_comment_sentiment = None
                anps_sentiment_score = None
                anps_joy = None
                anps_anger = None
                anps_disgust = None
                anps_sadness = None
                anps_fear = None
        else:
            anps_comment_sentiment = None
            anps_sentiment_score = None
            anps_joy = None
            anps_anger = None
            anps_disgust = None
            anps_sadness = None
            anps_fear = None

        new_line = [
            survey_id,
            nps_comment_sentiment,
            nps_sentiment_score,
            nps_joy,
            nps_anger,
            nps_sadness,
            nps_disgust,
            nps_fear,
            anps_comment_sentiment,
            anps_sentiment_score,
            anps_joy,
            anps_anger,
            anps_sadness,
            anps_disgust,
            anps_fear,
        ]
        new_line_check = new_line[1:]
        if all(v is None for v in new_line_check) is False:
            watson_data['analysis'].append(new_line)
        print_progress(j+1, 30000 - reviewed_data['metrics']['query_count'], prefix='Analyzing', suffix='Complete')
    save_json(get_file(watson_data_file, 'data'), watson_data)
    reviewed_data['metrics']['query_count'] = query_count
    save_json(get_file(watson_reviewed_file, 'data'), reviewed_data)
    return watson_data['analysis']
