from word_list_generator import list_generator
from oracle_bridge.oracle_bridge import run_query

test_list = [
    'R_1o51ugJbLConOtV'
]

test_query = """
SELECT *
  FROM JDLAURET.T_NPS_SURVEY_RESPONSE TNSR
WHERE TNSR.RESPONSE_ID = \'{response_id}\'
"""
comments = []
for i in test_list:
    comments.append(run_query('','',raw_query=test_query.format(response_id=i), credentials='private'))

header = comments[0][0]
for j, line in enumerate(comments):
    comments[j] = line[1]

test_data = [header]+comments

word_list_test = list_generator(test_data, header, 1, testing=True)