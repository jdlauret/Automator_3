import re
import os
import sys
import string
import json
import nltk
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from nltk import word_tokenize, sent_tokenize

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from spell_checker import correction
from nlu_bridge import watson_comment_analysis

# NLTK functions used
# Stop words is a list of all stop words (e.g., the, a, etc..)
# WordNetLemmatizer changes the plural of a word to the singular definition (e.g., wolves => wolf, androids => android)
stop_words = set(stopwords.words('english'))
wnl = WordNetLemmatizer()
# english_vocab is a dict of all the words in the english language
english_vocab = set(w.lower() for w in nltk.corpus.words.words())

# Constants
promoter = 9
detractor = 6

# File names
excluded_word_file = 'excluded_words.json'
word_and_id_file = 'nps_word_pairs.json'
watson_data_file = 'watson_comment_data.json'


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
        Creates file string
        :param file_name:
        :param folder_name:
        :return: returns concatenated string
    """
    return os.path.join(find_main_dir(), folder_name + '/' + file_name)


# open all required files
with open(get_file(excluded_word_file, 'data')) as excluded_words,\
     open(get_file(word_and_id_file, 'data')) as word_and_id,\
     open(get_file(watson_data_file, 'data')) as watson_data:

    excluded_words = json.load(excluded_words)
    word_and_id = json.load(word_and_id)
    watson_data = json.load(watson_data)


def save_json(file, obj):
    """
    Saves an obj to a json
    :param file: name of the file to dump the data into
    :param obj: the object to dump
    """
    with open(file, 'w') as outfile:
        json.dump(obj, outfile, indent=4, sort_keys=True)


def has_numbers(input_string):
    """
    Checks to see if a string is a number
    :param input_string: String to check for numbers
    :return: Boolean
    """
    return bool(re.search(r'\d', input_string))


def key_builder(word_num):
    """
    generates a key for an object based on the input
    :param word_num: the number of word(s) being created
    :return: return the concatenated string
    """
    key_name = 'word_id_' + str(word_num)

    return key_name


def has_punctuation(word):
    return any(p in word for p in string.punctuation)


def word_check(word, word_num, survey_id, comment_type, obj_key):
    """
    Check to see if the submitted word should be used or discard, returns a boolean based on the result
    :param word: the word to evaluate
    :param word_num: number of words submitted
    :param survey_id: the survey id of the word submitted
    :param comment_type: the type of comment either NPS or aNPS
    :param obj_key: the storage location key for the json object
    :return: Returns a Boolean
    """
    new_word = word.lower().strip()

    if word_num < 2:

        if has_punctuation(new_word):
            return False

        elif new_word in stop_words:
            return False

        elif has_numbers(new_word):
            return False

        elif new_word in set(excluded_words['excluded_words']):
            return False

        elif word in word_and_id[survey_id][obj_key][comment_type]['Word_List']:
            return False

        else:
            return True


def remove_values(arr, val):
    """
    Check if all values in a list are unique
    :param arr: the list to check
    :param val: the value to check against
    :return: returns only unique items
    """
    return [value for value in arr if value != val]


def check_reviewed(key_name, survey_id, comment_type):
    """
    Check if the survey has been reviewed previously
    :param key_name: the obj key where the list is stored
    :param survey_id: the survey id that is being checked
    :param comment_type: the type of comment that is being checked
    :return: returns the stored boolean
    """
    return word_and_id[survey_id][key_name][comment_type]['Reviewed']


def spell_check(word):
    """
    Spell check a word
    Spell check accuracy ~70%
    If the word is stored in a excluded words libraries it will pass over them or correct a specific word to the
    correct value.
    :param word: The word to spell check
    :return: returns the corrected word
    """
    word_test = word.lower()
    if word_test in excluded_words['word_corrections'].keys():
        word_test = excluded_words['word_corrections'][word_test]

    if word_test not in excluded_words['pass_words'] \
            and has_numbers(word_test) is False:

        if word_test not in string.punctuation:
            word_test = wnl.lemmatize(word_test)

        if word_test not in english_vocab:
            word_test = correction(word_test)

        if word_test in excluded_words['word_corrections'].keys():
            word_test = excluded_words['word_corrections'][word_test]

    return word_test


def comment_handler(survey_id, comment, obj_key, word_num, comment_type):
    """
    First checks the number of words
    If the number is 1, Tokenizes words, Uses word_check to see if word is ok to use
    If word is usable, word is submitted to spell check then added to list
    If the number is >1, All above steps are still followed with the exception of
    allowing some stop words.  The number of stop words allowed is the word number minus 2.
    So if the number is 3, 1 stop word is allowed.
    :param survey_id: Survey ID of the word(s) be analyzed
    :param comment: The Comment from the survey
    :param obj_key: The storage key for the json object
    :param word_num: The number of words per set
    :param comment_type: The type of comment NPS or aNPS
    """
    if word_num == 1:
        text = word_tokenize(comment)

        for word in text:
            valid_word = word_check(word, word_num, survey_id, comment_type, obj_key)
            if valid_word:
                corrected_word = spell_check(word)
                if corrected_word != word:
                    valid_word = word_check(corrected_word, word_num, survey_id, comment_type, obj_key)
                if word not in word_and_id[survey_id][obj_key][comment_type]['Word_List'] \
                        and valid_word \
                        and len(corrected_word) > 2:
                    word_and_id[survey_id][obj_key][comment_type]['Word_List'].append(corrected_word)

    elif word_num > 1:
        temp_arr = list()
        sentences = sent_tokenize(comment)

        for sentence in sentences:
            temp_arr.append(word_tokenize(sentence))

        for line in temp_arr:
            start = 0
            stop = len(line)

            while start < stop:
                slice_start = start
                slice_end = start + word_num
                stop_words_allowed = word_num - 2
                stop_word_count = 0

                if (slice_end - 1) < stop:
                    pair = line[slice_start:slice_end]
                    word_test = []

                    for i, word in enumerate(pair):
                        result = word_check(word, word_num, survey_id, comment_type, obj_key)

                        if result:
                            pair[i] = spell_check(word)
                            word_test.append(result)
                        elif not result:
                            word_test.append(result)
                        elif result == 'stop_word':

                            if stop_word_count < stop_words_allowed:
                                word_test.append(True)
                            else:
                                word_test.append(False)

                    test = remove_values(word_test, False)

                    if len(test) == len(word_test):

                        pair = ' '.join(pair)
                        pair = pair.lower()
                        if pair not in word_and_id[survey_id][obj_key][comment_type]['Word_List']:
                            word_and_id[survey_id][obj_key][comment_type]['Word_List'].append(pair)

                start += 1


def watson_handler(comment, survey_id):
    """
    Submits data to Watson for NLU analysis
    Checks if the survey ID has been reviewed previously
    If the survey has not been analyzed submits it to Watson
    :param comment: The comment to analyze
    :param survey_id: The Survey ID to store
    :return:  Either the json from Watson or 'Already Analyzed'
    """
    survey_id_list = watson_data['survey_ids']
    if survey_id not in survey_id_list:
        watson_data['survey_ids'].append(survey_id)
        return watson_comment_analysis(comment)
    else:
        return 'Already Analyzed'


def list_generator(data, data_header, word_num, reset=False, testing=False):
    """
    Generates a list of words with survey_id and stores it in a json
    Returns the json storage key
    :param data: The NPS data to be analyzed
    :param data_header: The Header of the Data
    :param word_num: The number of words in a set to be generated
    :param reset: If TRUE will reset all stored data
    :return: json storage key
    """
    survey_id_col = -1
    nps_comment_col = -1
    anps_comment_col = -1

    if reset:
        save_json(get_file(word_and_id_file, 'data'), {})

    print()
    print('Building {0} Word Set(s)'.format(word_num))
    for i, col in enumerate(data_header):
        check = col.lower()
        if check == 'response_id':
            survey_id_col = i
        elif check == 'nps_comments':
            nps_comment_col = i
        elif check == 'anps_comments':
            anps_comment_col = i

    lines = len(data)
    print("Now building data from {0} surveys".format(lines))

    for j, line in enumerate(data):
        if (j+1) % 100 == 0:
            print('Analyzing Survey {0}'.format(j+1))
        if j > 0:
            survey_id = line[survey_id_col]
            nps_comment = str(line[nps_comment_col])
            anps_comment = str(line[anps_comment_col])
            key_name = key_builder(word_num)

            if survey_id not in word_and_id.keys():
                word_and_id[survey_id] = {}

            if key_name not in word_and_id[survey_id].keys():
                word_and_id[survey_id][key_name] = {
                    'NPS': {
                        'Reviewed': False,
                        'Word_List': []
                    },
                    'ANPS': {
                        'Reviewed': False,
                        'Word_List': []
                    }
                }

            if check_reviewed(key_name, survey_id, 'NPS') is False or testing:
                if nps_comment is not None and nps_comment != '':
                    nps_comment = str(nps_comment)
                    comment_handler(survey_id, nps_comment, key_name, word_num, 'NPS')

                if not testing:
                    word_and_id[survey_id][key_name]['NPS']['Reviewed'] = True

            if check_reviewed(key_name, survey_id, 'ANPS') is False or testing:
                if anps_comment is not None and anps_comment != '':
                    anps_comment = str(anps_comment)
                    comment_handler(survey_id, anps_comment, key_name, word_num, 'ANPS')

                if not testing:
                    word_and_id[survey_id][key_name]['ANPS']['Reviewed'] = True
    print('{0} Surveys Analyzed'.format(j + 1))

    if not testing:
        print('Saving Word Cloud Data')
        save_json(get_file(excluded_word_file, 'data'), excluded_words)
        save_json(get_file(word_and_id_file, 'data'), word_and_id)

        return key_name
