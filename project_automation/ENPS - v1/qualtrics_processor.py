import json
import os

from dateutil.parser import parse

data_warehouse = [
    [
        'RESPONSE_ID',
        'SURVEY_ENDED_AT',
        'SURVEY_TYPE',
        'SURVEY_ID',
        'MANAGEMENT_LEVEL',
        'DEPARTMENT',
        'OFFICE',
        'SURVEY_STATE',
        'PAY_TYPE',
        'HIRE_DATE',
        'GENDER',
        'MANAGER_NAME',
        'ENPS_SCORE',
        'ENPS_COMMENTS',
        'CONFIDENT_IN_STRATEGIC_DIR',
        'COMMITTED_TO_VSLR',
        'PROUD_TO_WORK_FOR_VSLR',
        'PASSIONATE_ABOUT_VSLR_GOALS',
        'JOB_MAKES_POSITIVE_DIFFERENCE',
        'EXCITED_TO_WORK',
        'SATISFIED_W_VSLR_CAREER_OPPS',
        'FEEL_LIKE_PART_OF_VSLR',
        'BELIEVE_WILL_BE_HERE_IN_A_YEAR',
        'BELIEVE_WILL_BE_HERE_IN_5_YRS',
        'UNDERSTAND_WHAT_MGR_EXPECTS',
        'MGR_HOLDS_ME_ACCOUNTABLE',
        'EMPOWERED_TO_WORK_EFFECTIVELY',
        'EQUIPPED_TO_PERFORM_DUTIES',
        'UNDERSTAND_VSLR_NEEDS_THIS_Q',
        'TEAM_HAS_CLEAR_INITIATIVES',
        'DEPT_HAS_HIGH_PERF_STANDARDS',
        'MGR_KEEPS_ME_INFORMED',
        'MGR_IS_QUALIFIED_FOR_JOB',
        'REGULARLY_RECEIVE_INFORMATION',
    ]
]

type_dict = {
    'EE (2016 Q3)': {
        'ENPS': {
            'ENPS Score': ['Q14'],
            'ENPS Comments': [
                'Q15',
            ]
        },
        'Response ID': {
            'Response ID': 'ResponseID',
            'Survey Type': 'EE (2016 Q3)',
            'Survey Finished': 'EndDate',
            'Survey ID': 'SV_0GQMKtGNlaWPseF',
            'Management Level': 'ManagementLevel',
            'Department': 'Department',
            'Office': 'Office',
            'State': 'State',
            'Pay Type': 'PayType',
            'Hire Date': 'Hire Date',
            'Gender': 'Gender',
            'Manager Name': 'Manager - Level 01',
            'Confident in strategic dir': 'Q6_1',
            'Committed to vslr': 'Q6_2',
            'Proud to work for vslr': 'Q6_5',
            'Passionate about vslr goals': 'Q6_6',
            'Job makes positive difference': '',
            'Excited to work': 'Q6_7',
            'Satisfied w vslr career opps': 'Q6_10',
            'Feel like part of vslr': 'Q6_3',
            'Believe will be here in a year': '',
            'Believe will be here in 5 yrs': 'Q6_11',
            'Understand what mgr expects': 'Q7_11',
            'Mgr holds me accountable': 'Q7_2',
            'Empowered to work effectively': 'Q7_3',
            'Equipped to perform duties': 'Q7_4',
            'Understand vslr need this q': 'Q7_5',
            'Team has clear initiatives': '',
            'Dept has high perf standards': 'Q7_6',
            'Mgr keeps me informed': 'Q7_7',
            'Mgr if qualified for job': 'Q7_9',
            'Regularly receive information': 'Q7_8',
        }
    },
    'EE (2017 Q1)': {
        'ENPS': {
            'ENPS Score': ['Q14'],
            'ENPS Comments': [
                'Q15',
                'Q1',
                'Q10',
            ]
        },
        'Response ID': {
            'Response ID': 'ResponseID',
            'Survey Type': 'EE (2017 Q1)',
            'Survey Finished': 'EndDate',
            'Survey ID': 'SV_8ALSvUiqYBqd57T',
            'Management Level': 'ManagementLevel',
            'Department': 'Department',
            'Office': 'Location',
            'State': 'State',
            'Pay Type': 'PayType',
            'Hire Date': 'HireDate',
            'Gender': 'Gender',
            'Manager Name': 'Manager - Level 01',
            'Confident in strategic dir': 'Q6_1',
            'Committed to vslr': 'Q6_2',
            'Proud to work for vslr': 'Q6_5',
            'Passionate about vslr goals': '',
            'Job makes positive difference': 'Q6_6',
            'Excited to work': 'Q6_7',
            'Satisfied w vslr career opps': 'Q6_10',
            'Feel like part of vslr': 'Q6_3',
            'Believe will be here in a year': 'Q6_13',
            'Believe will be here in 5 yrs': '',
            'Understand what mgr expects': 'Q7_11',
            'Mgr holds me accountable': 'Q7_12',
            'Empowered to work effectively': 'Q7_13',
            'Equipped to perform duties': 'Q7_14',
            'Understand vslr need this q': 'Q7_16',
            'Team has clear initiatives': 'Q7_17',
            'Dept has high perf standards': 'Q7_18',
            'Mgr keeps me informed': '',
            'Mgr if qualified for job': 'Q7_19',
            'Regularly receive information': 'Q7_20',
        }
    },
    'EE (2018 Q1)': {
        'ENPS': {
            'ENPS Score': ['Q14'],
            'ENPS Comments': [
                'Q15',
                'Q1',
                'Q10',
            ]
        },
        'Response ID': {
            'Response ID': 'ResponseID',
            'Survey Type': 'EE (2018 Q1)',
            'Survey Finished': 'EndDate',
            'Survey ID': 'SV_5p7X2Fo5b9xMZgh',
            'Management Level': 'ManagementLevel',
            'Department': 'Department',
            'Office': 'Location',
            'State': 'State',
            'Pay Type': 'PayType',
            'Hire Date': 'HireDate',
            'Gender': 'Gender',
            'Manager Name': 'Manager - Level 01',
            'Confident in strategic dir': 'Q6_1',
            'Committed to vslr': 'Q6_2',
            'Proud to work for vslr': 'Q6_5',
            'Passionate about vslr goals': '',
            'Job makes positive difference': 'Q6_6',
            'Excited to work': 'Q6_7',
            'Satisfied w vslr career opps': 'Q6_10',
            'Feel like part of vslr': 'Q6_3',
            'Believe will be here in a year': 'Q6_13',
            'Believe will be here in 5 yrs': '',
            'Understand what mgr expects': 'Q7_11',
            'Mgr holds me accountable': 'Q7_12',
            'Empowered to work effectively': 'Q7_13',
            'Equipped to perform duties': 'Q7_14',
            'Understand vslr need this q': 'Q7_16',
            'Team has clear initiatives': 'Q7_17',
            'Dept has high perf standards': 'Q7_18',
            'Mgr keeps me informed': '',
            'Mgr if qualified for job': 'Q7_19',
            'Regularly receive information': 'Q7_20',
        }
    }
}


def process_survey(survey_key, data):
    survey_info = type_dict[survey_key]

    header = data[0]
    del data[0:3]

    for row in data:
        response_id = row[header.index(survey_info['Response ID']['Response ID'])]
        survey_type = survey_info['Response ID']['Survey Type']
        survey_finished = row[header.index(survey_info['Response ID']['Survey Finished'])]
        survey_finished = parse(survey_finished)
        survey_id = survey_info['Response ID']['Survey ID']
        try:
            management_level = row[header.index(survey_info['Response ID']['Management Level'])]
        except:
            management_level = ''
        try:
            department = row[header.index(survey_info['Response ID']['Department'])]
        except:
            department = ''
        try:
            office = row[header.index(survey_info['Response ID']['Office'])]
        except:
            office = ''
        try:
            state = row[header.index(survey_info['Response ID']['State'])]
        except:
            state = ''
        try:
            pay_type = row[header.index(survey_info['Response ID']['Pay Type'])]
        except:
            pay_type = ''
        try:
            hire_date = row[header.index(survey_info['Response ID']['Hire Date'])]
        except:
            hire_date = ''
        try:
            gender = row[header.index(survey_info['Response ID']['Gender'])]
        except:
            gender = ''
        try:
            manager_name = row[header.index(survey_info['Response ID']['Manager Name'])]
        except:
            manager_name = ''
        try:
            q1 = str(6 - int(row[header.index(survey_info['Response ID']['Confident in strategic dir'])]))
        except:
            q1 = ''
        try:
            q2 = str(6 - int(row[header.index(survey_info['Response ID']['Committed to vslr'])]))
        except:
            q2 = ''
        try:
            q3 = str(6 - int(row[header.index(survey_info['Response ID']['Proud to work for vslr'])]))
        except:
            q3 = ''
        try:
            q4 = str(6 - int(row[header.index(survey_info['Response ID']['Passionate about vslr goals'])]))
        except:
            q4 = ''
        try:
            q5 = str(6 - int(row[header.index(survey_info['Response ID']['Job makes positive difference'])]))
        except:
            q5 = ''
        try:
            q6 = str(6 - int(row[header.index(survey_info['Response ID']['Excited to work'])]))
        except:
            q6 = ''
        try:
            q7 = str(6 - int(row[header.index(survey_info['Response ID']['Satisfied w vslr career opps'])]))
        except:
            q7 = ''
        try:
            q8 = str(6 - int(row[header.index(survey_info['Response ID']['Feel like part of vslr'])]))
        except:
            q8 = ''
        try:
            q9 = str(6 - int(row[header.index(survey_info['Response ID']['Believe will be here in a year'])]))
        except:
            q9 = ''
        try:
            q10 = str(6 - int(row[header.index(survey_info['Response ID']['Believe will be here in 5 yrs'])]))
        except:
            q10 = ''
        try:
            q11 = str(6 - int(row[header.index(survey_info['Response ID']['Understand what mgr expects'])]))
        except:
            q11 = ''
        try:
            q12 = str(6 - int(row[header.index(survey_info['Response ID']['Mgr holds me accountable'])]))
        except:
            q12 = ''
        try:
            q13 = str(6 - int(row[header.index(survey_info['Response ID']['Empowered to work effectively'])]))
        except:
            q13 = ''
        try:
            q14 = str(6 - int(row[header.index(survey_info['Response ID']['Equipped to perform duties'])]))
        except:
            q14 = ''
        try:
            q15 = str(6 - int(row[header.index(survey_info['Response ID']['Understand vslr need this q'])]))
        except:
            q15 = ''
        try:
            q16 = str(6 - int(row[header.index(survey_info['Response ID']['Team has clear initiatives'])]))
        except:
            q16 = ''
        try:
            q17 = str(6 - int(row[header.index(survey_info['Response ID']['Dept has high perf standards'])]))
        except:
            q17 = ''
        try:
            q18 = str(6 - int(row[header.index(survey_info['Response ID']['Mgr keeps me informed'])]))
        except:
            q18 = ''
        try:
            q19 = str(6 - int(row[header.index(survey_info['Response ID']['Mgr if qualified for job'])]))
        except:
            q19 = ''
        try:
            q20 = str(6 - int(row[header.index(survey_info['Response ID']['Regularly receive information'])]))

        except:
            q20 = ''

        enps_score = max(row[header.index(comment)] for comment in survey_info['ENPS']['ENPS Score'])
        enps_comment = [row[header.index(comment)] for comment in survey_info['ENPS']['ENPS Comments']]
        try:
            enps_comment = list(filter(None, enps_comment))[0].replace('\\\\', '\n')
        except:
            enps_comment = ''

        data_warehouse_row = [
            response_id,
            survey_finished,
            survey_id,
            management_level,
            department,
            office,
            state,
            pay_type,
            hire_date,
            gender,
            manager_name,
            enps_score,
            enps_comment,
            q1,
            q2,
            q3,
            q4,
            q5,
            q6,
            q7,
            q8,
            q9,
            q10,
            q11,
            q12,
            q13,
            q14,
            q15,
            q16,
            q17,
            q18,
            q19,
            q20,
            survey_type
        ]

        if data_warehouse_row not in data_warehouse:
            data_warehouse.append(data_warehouse_row)


def correction_set_one(row, header, survey_type):
    new_row = row.copy()
    if survey_type == 'onboarding':
        first_row_corrections = [
            header.index('SALES_SHOWN_WHERE_CORRELATION'),
            header.index('SALES_SHOWN_ONLINE_TRAINING'),
            header.index('SALES_SHOWN_WHERE_BOOT_CAMP'),
            header.index('SALES_SHOWN_HOW_CONTACT_MGR'),
            header.index('SHOWN_WHERE_ORIENTATION'),
            header.index('SHOWN_ONLINE_TRAINING'),
            header.index('SHOWN_WHERE_NEW_HIRE_TRAINING'),
            header.index('SHOWN_HOW_CONTACT_MGR'),
            header.index('SHADOWED_SOMEONE'),
            header.index('SALES_COMPLETED_BOOT_CAMP'),
        ]
        for correction in first_row_corrections:
            if new_row[correction] == 1:
                new_row[correction] = 1
            elif new_row[correction] == 2:
                new_row[correction] = 0
            else:
                new_row[correction] = ''
    elif survey_type == 'exit':
        first_row_corrections = [
            header.index('LEAVING_TO_VSLR_COMPETITOR')
        ]
        for correction in first_row_corrections:
            if new_row[correction] == 23:
                new_row[correction] = 1
            elif new_row[correction] == 24:
                new_row[correction] = 0
            else:
                new_row[correction] = ''

    return new_row


def correction_set_two(row, header, survey_type):
    new_row = row.copy()
    if survey_type == 'onboarding':
        second_row_corrections = [
            header.index('SALES_COMPLETED_TRAINING'),
            header.index('COMPLETED_TRAINING'),
        ]
        for correction in second_row_corrections:
            if new_row[correction] == 1:
                new_row[correction] = "Yes"
            elif new_row[correction] == 2:
                new_row[correction] = 'No'
            elif new_row[correction] == 3:
                new_row[correction] = 'Partial'
            else:
                new_row[correction] = ''
    elif survey_type == 'exit':
        second_row_corrections = [
            header.index('PRIMARY_REASON_LEAVING')
        ]
        for correction in second_row_corrections:
            if str(row[correction]) == '1':
                row[correction] = 'Dissatisfied with Pay'
            elif str(row[correction]) == '2':
                row[correction] = 'Dissatisfied with Benefits'
            elif str(row[correction]) == '3':
                row[correction] = 'Personal/Family Reasons'
            elif str(row[correction]) == '4':
                row[correction] = 'Dissatisfied with Manager/Supervisor'
            elif str(row[correction]) == '5':
                row[correction] = 'Better Job Opportunity'
            elif str(row[correction]) == '6':
                row[correction] = 'Scheduling/School Conflict'
            elif str(row[correction]) == '7':
                row[correction] = 'Job Did Not Meet Expectations'
            elif str(row[correction]) == '8':
                row[correction] = 'Lack of Training & Development'
            elif str(row[correction]) == '9':
                row[correction] = 'Career Growth/Opportunity'
            elif str(row[correction]) == '10':
                row[correction] = 'Other'
            elif str(row[correction]) == '11':
                row[correction] = 'Dissatisfied with Product Offerings'
            elif str(row[correction]) == '12':
                row[correction] = 'Dissatisfied with Process/Policies'
            else:
                row[correction] = ''
    return new_row


def correction_set_three(row, header):
    new_row = row.copy()
    third_row_corrections = [
        header.index('HAPPY_TO_JOIN_VSLR'),
        header.index('BELIEVE_WILL_BE_HERE_IN_A_YEAR'),
        header.index('MGR_GIVEN_ADEQUATE_SUPPORT'),
        header.index('KNOW_WHERE_TO_GET_HELP'),
        header.index('HAVE_TRAINING_I_NEED'),
    ]
    for correction in third_row_corrections:
        if new_row[correction] != '':
            new_row[correction] = 61 - int(new_row[correction])
    return new_row


def correction_set_four(row, header):
    new_row = row.copy()
    fourth_row_corrections = [
        header.index('POSITIVE_ONBOARDING_EXPERIENCE'),
        header.index('SALES_UNDERSTAND_COMPENSATION'),
        header.index('SALES_UNDERSTAND_NEO'),
        header.index('SALES_UNDERSTAND_SALES_PROCESS'),
        header.index('SALES_KNOW_HOW_RESOLVE_ISSUES'),
        header.index('UNDERSTAND_HOW_PAID'),
        header.index('UNDERSTAND_WORKDAY'),
    ]
    for correction in fourth_row_corrections:
        if new_row[correction] != '':
            new_row[correction] = 6 - int(new_row[correction])
    return new_row


def onboarding_processing(data):
    header = data[0]
    del data[0:3]

    onboarding_data = [
        [
            'RESPONSE_ID', 'SURVEY_ENDED_AT', 'SURVEY_TYPE', 'SURVEY_ID', 'EMPLOYEE_ID', 'MANAGEMENT_LEVEL',
            'COST_CENTER_HIERARCHY', 'DEPARTMENT', 'MANAGER_NAME', 'GENDER', 'PAY_TYPE', 'OFFICE', 'SURVEY_STATE',
            'HIRE_DATE', 'ENPS_SCORE', 'ENPS_COMMENTS', 'HAPPY_TO_JOIN_VSLR', 'BELIEVE_WILL_BE_HERE_IN_A_YEAR',
            'FACTORS_3_COMPENSATION', 'FACTORS_3_BENEFITS', 'FACTORS_3_SALES_PROCESS_EASE',
            'FACTORS_3_POLICY_AND_PROCESSES', 'FACTORS_3_MANAGEMENT', 'FACTORS_3_CAREER_GROWTH', 'FACTORS_3_TRAINING',
            'FACTORS_3_CULTURE', 'FACTORS_3_TYPE_OF_WORK', 'FACTORS_3_COMPANY_GROWTH', 'FACTORS_3_COMPETITIVE_PRODUCT',
            'FACTORS_3_OTHER', 'FACTORS_3_OTHER_TEXT', 'FIRST_WKS_REINFORCED_DECISION', 'FIRST_WKS_REINFORCED_WHY',
            'POSITIVE_ONBOARDING_EXPERIENCE', 'POSITIVE_ONBOARDING_WHY', 'SALES_SHOWN_WHERE_CORRELATION',
            'SALES_SHOWN_ONLINE_TRAINING', 'SALES_SHOWN_WHERE_BOOT_CAMP', 'SALES_SHOWN_HOW_CONTACT_MGR',
            'SHOWN_WHERE_ORIENTATION', 'SHOWN_ONLINE_TRAINING', 'SHOWN_WHERE_NEW_HIRE_TRAINING',
            'SHOWN_HOW_CONTACT_MGR', 'SALES_COMPLETED_TRAINING', 'COMPLETED_TRAINING', 'TRAINING_EFFECTIVENESS',
            'TRAINING_WHAT_MOST_HELPFUL', 'TRAINING_WHAT_MISSING', 'SALES_UNDERSTAND_COMPENSATION',
            'SALES_UNDERSTAND_NEO', 'SALES_UNDERSTAND_SALES_PROCESS', 'SALES_KNOW_HOW_RESOLVE_ISSUES',
            'UNDERSTAND_HOW_PAID', 'UNDERSTAND_WORKDAY', 'UNDERSTAND_HOW_VSLR_OPERATES', 'SHADOWED_SOMEONE',
            'SHADOWED_WHY_NOT', 'SALES_COMPLETED_BOOT_CAMP', 'SALES_BOOT_CAMP_WHY_NOT', 'MGR_GIVEN_ADEQUATE_SUPPORT',
            'KNOW_WHERE_TO_GET_HELP', 'HAVE_TRAINING_I_NEED', 'WHAT_ADDITIONAL_TRAINING', 'CHANGE_MOST_POSITIVE_IMPACT'
        ]
    ]
    for row in data:
        # Temp Solution to remove jacked up lines
        if row[header.index('Manager ID')].lower() != 'ip address':
            new_row = [
                row[header.index('ResponseID')],
                row[header.index('EndDate')],
                'Onboarding (2017)',
                'SV_6o2ccgTXfhSpYAR',
                row[header.index('Employee ID')],
                row[header.index('Management Level')],
                row[header.index('Cost Center Hierarchy')],
                row[header.index('Department')],
                row[header.index('Manager Name')],
                row[header.index('Gender')],
                row[header.index('Pay Type')],
                row[header.index('Office')],
                row[header.index('State')],
                row[header.index('Hire Date')],
                row[header.index('Q2')],
                ''.join([row[header.index('Q3')], row[header.index('Q4')], row[header.index('Q5')]]),
                row[header.index('Q6_1')],
                row[header.index('Q6_2')],
                row[header.index('Q7_1')],
                row[header.index('Q7_12')],
                row[header.index('Q7_2')],
                row[header.index('Q7_3')],
                row[header.index('Q7_4')],
                row[header.index('Q7_5')],
                row[header.index('Q7_6')],
                row[header.index('Q7_7')],
                row[header.index('Q7_8')],
                row[header.index('Q7_11')],
                row[header.index('Q7_9')],
                row[header.index('Q7_10')],
                row[header.index('Q7_10_TEXT')],
                row[header.index('Q8')],
                row[header.index('Q9')],
                row[header.index('Q10')],
                row[header.index('Q11')],
                row[header.index('Q12_1')],
                row[header.index('Q12_2')],
                row[header.index('Q12_3')],
                row[header.index('Q12_4')],
                row[header.index('Q13_1')],
                row[header.index('Q13_2')],
                row[header.index('Q13_3')],
                row[header.index('Q13_4')],
                row[header.index('Q14')],
                row[header.index('Q15')],
                row[header.index('Q16')],
                row[header.index('Q17')],
                row[header.index('Q18')],
                row[header.index('Q19_1')],
                row[header.index('Q19_2')],
                row[header.index('Q19_3')],
                row[header.index('Q19_4')],
                row[header.index('Q85_1')],
                row[header.index('Q85_2')],
                row[header.index('Q85_3')],
                row[header.index('Q20')],
                row[header.index('Q21')],
                row[header.index('Q22')],
                row[header.index('Q23')],
                row[header.index('Q24_1')],
                row[header.index('Q24_2')],
                row[header.index('Q24_3')],
                row[header.index('Q25')],
                row[header.index('Q26')],
            ]
            onboarding_data.append(new_row)
    onboarding_header = onboarding_data[0]
    for i, row in enumerate(onboarding_data):
        if row != onboarding_header:
            onboarding_data[i] = correction_set_one(
                correction_set_two(
                    correction_set_three(
                        correction_set_four(onboarding_data[i], onboarding_header),
                        onboarding_header),
                    onboarding_header, 'onboarding'),
                onboarding_header, 'onboarding')

    return onboarding_data


def exit_processing(data):
    header = data[0]
    del data[0:3]

    exit_data = [
        [
            'RESPONSE_ID', 'SURVEY_ENDED_AT', 'SURVEY_TYPE', 'SURVEY_ID', 'EMPLOYEE_ID', 'MANAGEMENT_LEVEL',
            'COST_CENTER_HIERARCHY', 'DEPARTMENT', 'MANAGER_NAME', 'GENDER', 'PAY_TYPE', 'OFFICE', 'SURVEY_STATE',
            'HIRE_DATE', 'TERM_DATE', 'TERM_TYPE', 'TERM_REASON', 'REASON_FOR_LEAVING', 'CONCERNS_SHARED_WITH',
            'FACTOR_CAREER_ADVANCEMENT', 'FACTOR_WORK_SCHEDULE', 'FACTOR_TRAINING', 'FACTOR_BETTER_PAY',
            'FACTOR_MORE_CONSISTENT_PAY', 'FACTOR_BENEFITS', 'FACTOR_DIRECT_MANAGEMENT', 'FACTOR_COMMUTING_TIME',
            'FACTOR_JOB_DUTIES', 'FACTOR_COMPANY_CULTURE', 'FACTOR_RECOGNITION', 'FACTOR_PROCESSES_OR_POLICIES',
            'FACTOR_CLEARER_STRATEGIC_DIR', 'FACTOR_INDUSTRY_STABILITY', 'FACTOR_WORK_STRESS_BURNOUT',
            'FACTOR_INTERNAL_COMMUNICATION', 'FACTOR_COMPETITIVE_PRODUCT', 'CHANGE_WOULD_MAKE_A_DIFFERENCE',
            'PRIMARY_REASON_LEAVING', 'PRIMARY_REASON_LEAVING_OTHER', 'LEAVING_TO_VSLR_COMPETITOR',
            'WHAT_DOES_NEW_JOB_OFFER', 'SHARED_CONCERNS_W_MANAGER', 'SHARED_CONCERNS_W_COWORKER',
            'SHARED_CONCERNS_W_HR_PARTNER', 'SHARED_CONCERNS_W_OTHER', 'SHARED_CONCERNS_W_NOBODY',
            'SHARED_CONCERNS_OTHER', 'ADDITIONAL_INSIGHTS', 'ENPS_SCORE'
        ]
    ]
    for row in data:
        # Temp Solution to remove jacked up lines
        if row[header.index('Manager ID')].lower() != 'ip address':
            new_row = [
                row[header.index('ResponseID')],
                row[header.index('EndDate')],
                'Exit (2017)',
                'SV_0x24srHabajaRXn',
                row[header.index('Employee ID')],
                row[header.index('Management Level')],
                row[header.index('Cost Center Hierarchy')],
                row[header.index('Department')],
                row[header.index('Manager Name')],
                row[header.index('Gender')],
                row[header.index('Pay Type')],
                row[header.index('Office')],
                row[header.index('State')],
                row[header.index('Hire Date')],
                row[header.index('Term Date')],
                row[header.index('Term Type')],
                row[header.index('Term Reason')],
                row[header.index('Other Text - Reason for Leaving')],
                row[header.index('Other Text - Who Share Concerns With')],
                row[header.index('Q6_1')],
                row[header.index('Q6_2')],
                row[header.index('Q6_3')],
                row[header.index('Q6_4')],
                row[header.index('Q6_16')],
                row[header.index('Q6_5')],
                row[header.index('Q6_6')],
                row[header.index('Q6_7')],
                row[header.index('Q6_8')],
                row[header.index('Q6_9')],
                row[header.index('Q6_10')],
                row[header.index('Q6_11')],
                row[header.index('Q6_12')],
                row[header.index('Q6_13')],
                row[header.index('Q6_14')],
                row[header.index('Q6_15')],
                row[header.index('Q6_17')],
                row[header.index('Q3')],
                row[header.index('Q2')],
                row[header.index('Q2_TEXT')],
                row[header.index('Q4')],
                row[header.index('Q5')],
                row[header.index('Q8_1')],
                row[header.index('Q8_2')],
                row[header.index('Q8_3')],
                row[header.index('Q8_4')],
                row[header.index('Q8_5')],
                row[header.index('Q8_4_TEXT')],
                row[header.index('Q7')],
                row[header.index('Q9')],
                row[header.index('Termination Type')],
                row[header.index('Termination Reason')]
            ]
            exit_data.append(new_row)
    exit_header = exit_data[0]
    for i, row in enumerate(exit_data):
        if row != exit_header:
            exit_data[i] = correction_set_one(
                correction_set_two(row, exit_header, 'exit'),
                exit_header, 'exit')

    return exit_data


def qualtrics_processor(request):
    if request == 'data_warehouse':
        for filename in os.listdir('data\\qualtrics_jsons'):
            if '.json' in filename:
                fn = 'data\\qualtrics_jsons\\' + filename
                key = filename.replace('.json', '')
                with open(fn, 'r') as outfile:
                    data = json.load(outfile)
                data = data[key]
            if key == 'Turnover Report for EE August 2016':
                process_survey('EE (2016 Q3)', data)
            elif key == 'March 2017 (Historical) Survey':
                process_survey('EE (2017 Q1)', data)
            elif key == 'March 2018 Employee Engagement VSLR Form':
                process_survey('EE (2018 Q1)', data)

        return data_warehouse

    elif request == 'onboarding':
        with open('data\\qualtrics_jsons\\Onboarding (New) Survey.json', 'r') as outfile:
            data = json.load(outfile)
        data = data['Onboarding (New) Survey']
        return onboarding_processing(data)
    elif request == 'exit':
        with open('data\\qualtrics_jsons\\Exit (New) Survey.json', 'r') as outfile:
            data = json.load(outfile)
        data = data['Exit (New) Survey']
        return exit_processing(data)


def remove_non_ascii_2(text):
    return ''.join([i if ord(i) < 128 else '' for i in text])


def is_date(string):
    try:
        parse(string)
        return True
    except ValueError:
        return False


def is_number(string):
    try:
        float(string)
        return True
    except (TypeError, ValueError):
        pass

    try:
        import unicodedata
        unicodedata.numeric(string)
        return True
    except (TypeError, ValueError):
        pass
    return False


def format_survey_data(data):
    print()

    for i, row in enumerate(data):
        for j, cell in enumerate(row):
            s = cell
            if isinstance(s, str):
                data[i][j] = remove_non_ascii_2(s)

    return data
