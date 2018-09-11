import json
import os
import sys
from dateutil.parser import parse


def find_main_dir():
    if getattr(sys, 'frozen', False):
        # The application is frozen
        return os.path.dirname(sys.executable)
    else:
        # The application is not frozen
        # Change this bit to match where you store your data files:
        return os.path.dirname(__file__)


public_data_warehouse = [
    [
        'RESPONSE_ID',
        'CONTACT_ID',
        'SURVEY_ID',
        'PROJECT_ID',
        'EMPLOYEE_USER_ID',
        'SURVEY_TYPE',
        'NPS_SCORE',
        'ANPS_SCORE',
        'ANPS_SCORE_2',
        'NPS_COMMENTS',
        'ANPS_COMMENTS',
        'ANPS_COMMENTS_2',
        'SURVEY_ENDED_AT',
        'COMMUNICATION',
        'QUALITY_OF_WORK',
        'PROCESS_TIME',
        'IN_PERSON_INTERACTIONS',
        'UNDERSTANDING',
        'URGENCY',
        'PERSONAL_EFFORT',
        'ACCOUNT_CENTER_EASE',
        'SAVINGS',
        'VIVINT_EMPLOYEE_ID',
        'VIVINT_EMPLOYEE_ID_2',
        'CASE_ID',
        'BADGE_ID',
        'SERVICE_ID',
        'WORK_ORDER_ID',
        'TASK_ID',
        'MAY_CONTACT_FOR_FOLLOW_UP',
        'WOULD_LIKE_TO_BE_CONTACTED',
        'JOB_PROFILE'
    ]
]

type_dict = {
    'AFHD': {
        'NPS': {
            'Response ID': 'ResponseID',
            'NPS Type': 'NPS',
            'NPS Score': ['QID1'],
            'NPS Comments': [
                'QID2',
                'QID7',
                'QID8'
            ]
        },
        'ANPS': {
            'Response ID': 'ResponseID',
            'NPS': 'ANPS',
            'NPS Score': ['QID3'],
            'NPS Comments': [
                'QID4',
                'QID11',
                'QID10'
            ]
        },
        'Response ID': {
            'Response ID': 'ResponseID',
            'Survey Type': '550 - At-Fault Home Damage',
            'Survey Finished': 'EndDate',
            'Communication': 'QID15_1',
            'Quality of Work': '',
            'Process Time': '',
            'In Person Interactions': '',
            'Understanding': '',
            'Contact ID': 'Contact ID',
            'Project ID': 'Project ID',
            'Employee ID': 'Employee ID',
            'Vivint ID': '',
            'Case ID': 'Case ID',
            'Service ID': 'Service ID',
            'Badge ID': '',
            'Task ID': '',
            'Work Order ID': '',
            'Survey ID': 'SV_0032fulVyVV5rEN',
            'Vivint ID 2': '',
            'Urgency': 'QID15_5',
            'Personal Effort': '',
            'Account Center Ease': '',
            'Savings': '',
            'May Contact': 'QID6',
            'Wants to be Contacted': 'QID13',
        }
    },
    'CS': {
        'NPS': {
            'Response ID': 'ResponseID',
            'NPS Type': 'NPS',
            'NPS Score': ['QID1'],
            'NPS Comments': [
                'QID2',
                'QID7',
                'QID8'
            ]
        },
        'ANPS': {
            'Response ID': 'ResponseID',
            'NPS': 'ANPS',
            'NPS Score': ['Q16'],
            'NPS Comments': [
                'Q18',
                'Q19',
                'Q20'
            ]
        },
        'Response ID': {
            'Response ID': 'ResponseID',
            'Survey Type': '520 - Customer Service',
            'Survey Finished': 'EndDate',
            'Communication': '',
            'Quality of Work': '',
            'Process Time': '',
            'In Person Interactions': '',
            'Understanding': '',
            'Contact ID': 'Contact ID',
            'Project ID': 'Project ID',
            'Employee ID': 'Agent ID',
            'Vivint ID': '',
            'Case ID': '',
            'Service ID': 'Service ID',
            'Badge ID': '',
            'Task ID': 'Task ID',
            'Work Order ID': '',
            'Survey ID': 'SV_0cQts5G0eOs5pOZ',
            'Vivint ID 2': '',
            'Urgency': '',
            'Personal Effort': '',
            'Account Center Ease': '',
            'Savings': '',
            'May Contact': '',
            'Wants to be Contacted': 'QID13',
        }
    },
    'CSM': {
        'NPS': {
            'Response ID': 'ResponseID',
            'NPS Type': 'NPS',
            'NPS Score': ['QID1'],
            'NPS Comments': [
                'QID2',
                'QID7',
                'QID8'
            ]
        },
        'ANPS': {
            'Response ID': 'ResponseID',
            'NPS': 'ANPS',
            'NPS Score': ['QID3'],
            'NPS Comments': [
                'QID4',
                'QID11',
                'QID10'
            ]
        },
        'Response ID': {
            'Response ID': 'ResponseID',
            'Survey Type': '330 - Customer Success Managers',
            'Survey Finished': 'EndDate',
            'Communication': '',
            'Quality of Work': '',
            'Process Time': '',
            'In Person Interactions': '',
            'Understanding': '',
            'Contact ID': 'CONTACT_ID',
            'Project ID': 'PROJECT_ID',
            'Employee ID': 'LATEST_CREATED_BY_ID',
            'Vivint ID': '',
            'Case ID': '',
            'Service ID': 'SERVICE_ID',
            'Badge ID': '',
            'Task ID': 'LATEST_TASK_ID',
            'Work Order ID': '',
            'Survey ID': 'SV_6WlagN6igyFaO7r',
            'Vivint ID 2': '',
            'Urgency': '',
            'Personal Effort': '',
            'Account Center Ease': '',
            'Savings': '',
            'May Contact': 'QID6',
            'Wants to be Contacted': 'QID13',
        }
    },
    'ERCC': {
        'NPS': {
            'Response ID': 'ResponseID',
            'NPS Type': 'NPS',
            'NPS Score': ['QID1'],
            'NPS Comments': [
                'QID2',
                'QID7',
                'QID8'
            ]
        },
        'ANPS': {
            'Response ID': 'ResponseID',
            'NPS': 'ANPS',
            'NPS Score': ['QID3'],
            'NPS Comments': [
                'QID4',
                'QID11',
                'QID10'
            ]
        },
        'Response ID': {
            'Response ID': 'ResponseID',
            'Survey Type': '530 - Executive Resolutions',
            'Survey Finished': 'EndDate',
            'Communication': 'QID15_1',
            'Quality of Work': 'QID15_6',
            'Process Time': 'QID15_2',
            'In Person Interactions': 'QID15_5',
            'Understanding': 'QID15_3',
            'Contact ID': 'Contact ID',
            'Project ID': 'Project ID',
            'Employee ID': 'Case Owner ID',
            'Vivint ID': '',
            'Case ID': 'Case ID',
            'Service ID': 'Service ID',
            'Badge ID': '',
            'Task ID': '',
            'Work Order ID': '',
            'Survey ID': 'SV_71K480bakSFDmQd',
            'Vivint ID 2': '',
            'Urgency': '',
            'Personal Effort': '',
            'Account Center Ease': '',
            'Savings': '',
            'May Contact': '',
            'Wants to be Contacted': '',
        }
    },
    'FC': {
        'NPS': {
            'Response ID': 'ResponseID',
            'NPS Type': 'NPS',
            'NPS Score': ['QID1'],
            'NPS Comments': [
                'QID2',
                'QID7',
                'QID8'
            ]
        },
        'ANPS': {
            'Response ID': 'ResponseID',
            'NPS': 'ANPS',
            'NPS Score': ['QID3'],
            'NPS Comments': [
                'QID4',
                'QID11',
                'QID10'
            ]
        },
        'Response ID': {
            'Response ID': 'ResponseID',
            'Survey Type': '400 - FIN Complete',
            'Survey Finished': 'EndDate',
            'Communication': 'QID15_1',
            'Quality of Work': 'QID15_6',
            'Process Time': 'QID15_2',
            'In Person Interactions': 'QID15_5',
            'Understanding': 'QID15_3',
            'Contact ID': 'Contact ID',
            'Project ID': 'Project ID',
            'Employee ID': '',
            'Vivint ID': '',
            'Case ID': '',
            'Service ID': 'Service ID',
            'Badge ID': '',
            'Task ID': '',
            'Work Order ID': '',
            'Survey ID': 'SV_23rGU3M7mVXWfn7',
            'Vivint ID 2': '',
            'Urgency': '',
            'Personal Effort': '',
            'Account Center Ease': '',
            'Savings': '',
            'May Contact': 'QID6',
            'Wants to be Contacted': 'QID13',
        }
    },
    'HU': {
        'NPS': {
            'Response ID': 'ResponseID',
            'NPS Type': 'NPS',
            'NPS Score': ['QID1'],
            'NPS Comments': [
                'QID2',
                'QID7',
                'QID8'
            ]
        },
        'ANPS': {
            'Response ID': 'ResponseID',
            'NPS': 'ANPS',
            'NPS Score': ['QID3'],
            'NPS Comments': [
                'QID4',
                'QID11',
                'QID10'
            ]
        },
        'Response ID': {
            'Response ID': 'ResponseID',
            'Survey Type': '250 - Home Upgrade',
            'Survey Finished': 'EndDate',
            'Communication': 'QID15_1',
            'Quality of Work': 'QID15_6',
            'Process Time': 'QID15_2',
            'In Person Interactions': 'QID15_5',
            'Understanding': 'QID15_3',
            'Contact ID': 'Contact ID',
            'Project ID': 'Project ID',
            'Employee ID': '',
            'Vivint ID': '',
            'Case ID': 'Case ID',
            'Service ID': 'Service ID',
            'Badge ID': '',
            'Task ID': '',
            'Work Order ID': '',
            'Survey ID': 'SV_2tv9qp2qegueuln',
            'Vivint ID 2': '',
            'Urgency': '',
            'Personal Effort': '',
            'Account Center Ease': '',
            'Savings': '',
            'May Contact': 'QID6',
            'Wants to be Contacted': 'QID13',
        }
    },
    'ICO': {
        'NPS': {
            'Response ID': 'ResponseID',
            'NPS Type': 'NPS',
            'NPS Score': ['QID1'],
            'NPS Comments': [
                'QID2',
                'QID7',
                'QID8'
            ]
        },
        'ANPS': {
            'Response ID': 'ResponseID',
            'NPS': 'ANPS',
            'NPS Score': ['QID3'],
            'NPS Comments': [
                'QID4',
                'QID11',
                'QID10'
            ]
        },
        'ANPS2': {
            'Response ID': 'ResponseID',
            'NPS': 'ANPS2',
            'NPS Score': ['Q16'],
            'NPS Comments': [
                'Q17',
                'Q18',
                'Q19'
            ]
        },
        'Response ID': {
            'Response ID': 'ResponseID',
            'Survey Type': '300 - Install Complete',
            'Survey Finished': 'EndDate',
            'Communication': 'QID15_1',
            'Quality of Work': 'QID15_6',
            'Process Time': 'QID15_2',
            'In Person Interactions': 'QID15_5',
            'Understanding': 'QID15_3',
            'Contact ID': 'Contact ID',
            'Project ID': 'Project ID',
            'Employee ID': '',
            'Vivint ID': 'Employee ID',
            'Case ID': '',
            'Service ID': 'Service ID',
            'Badge ID': '',
            'Task ID': '',
            'Work Order ID': '',
            'Survey ID': 'SV_71XYFqoWgqrp8Md',
            'Vivint ID 2': 'Sales Rep ID',
            'Urgency': '',
            'Personal Effort': '',
            'Account Center Ease': '',
            'Savings': '',
            'May Contact': 'QID6',
            'Wants to be Contacted': 'QID13',
        }
    },
    'ICA': {
        'NPS': {
            'Response ID': 'ResponseID',
            'NPS Type': 'NPS',
            'NPS Score': [
                'QID1',
                'Q17',
                'Q28'
            ],
            'NPS Comments': [
                'QID2',
                'QID7',
                'QID8',
                'Q18',
                'Q19',
                'Q20',
                'Q29',
                'Q30',
                'Q31'
            ]
        },
        'ANPS': {
            'Response ID': 'ResponseID',
            'NPS': 'ANPS',
            'NPS Score': [
                'QID3',
                'Q21',
                'Q32'
            ],
            'NPS Comments': [
                'QID4',
                'QID11',
                'QID10',
                'Q22',
                'Q23',
                'Q24',
                'Q33',
                'Q34',
                'Q35'
            ]
        },
        'Response ID': {
            'Response ID': 'ResponseID',
            'Survey Type': '310 - Intro Call',
            'Survey Finished': 'EndDate',
            'Communication': '',
            'Quality of Work': '',
            'Process Time': '',
            'In Person Interactions': '',
            'Understanding': '',
            'Contact ID': 'CONTACT_ID',
            'Project ID': 'PROJECT_ID',
            'Employee ID': 'ID',
            'Vivint ID': '',
            'Case ID': '',
            'Service ID': 'SERVICE_ID',
            'Badge ID': '',
            'Task ID': 'TASK_ID',
            'Work Order ID': '',
            'Survey ID': 'SV_0ewdZ9Dh6U5ouax',
            'Vivint ID 2': '',
            'Urgency': '',
            'Personal Effort': '',
            'Account Center Ease': '',
            'Savings': '',
            'May Contact': 'Q36',
            'Wants to be Contacted': 'Q37',
        }
    },
    'PIP': {
        'NPS': {
            'Response ID': 'ResponseID',
            'NPS Type': 'NPS',
            'NPS Score': ['QID1'],
            'NPS Comments': [
                'QID2',
                'QID7',
                'QID8'
            ]
        },
        'ANPS': {
            'Response ID': 'ResponseID',
            'NPS': 'ANPS',
            'NPS Score': ['QID3'],
            'NPS Comments': [
                'QID4',
                'QID11',
                'QID10'
            ]
        },
        'Response ID': {
            'Response ID': 'ResponseID',
            'Survey Type': '320 - Post-Install Process',
            'Survey Finished': 'EndDate',
            'Communication': 'QID15_1',
            'Quality of Work': 'QID15_6',
            'Process Time': 'QID15_2',
            'In Person Interactions': 'QID15_5',
            'Understanding': 'QID15_3',
            'Contact ID': 'ContactId',
            'Project ID': 'ProjectId',
            'Employee ID': '',
            'Vivint ID': '',
            'Case ID': '',
            'Service ID': 'ServiceId',
            'Badge ID': '',
            'Task ID': '',
            'Work Order ID': '',
            'Survey ID': 'SV_eFnvsJjhOOPVowJ',
            'Vivint ID 2': '',
            'Urgency': '',
            'Personal Effort': '',
            'Account Center Ease': '',
            'Savings': '',
            'May Contact': 'QID6',
            'Wants to be Contacted': 'QID13',
        }
    },
    'PPWO': {
        'NPS': {
            'Response ID': 'ResponseID',
            'NPS Type': 'NPS',
            'NPS Score': ['QID1'],
            'NPS Comments': [
                'QID2',
                'QID7',
                'QID8'
            ]
        },
        'ANPS': {
            'Response ID': 'ResponseID',
            'NPS': 'ANPS',
            'NPS Score': ['QID3'],
            'NPS Comments': [
                'QID4',
                'QID11',
                'QID10'
            ]
        },
        'Response ID': {
            'Response ID': 'ResponseID',
            'Survey Type': '540 - Post-PTO Work Order',
            'Survey Finished': 'EndDate',
            'Communication': 'QID15_1',
            'Quality of Work': 'QID15_6',
            'Process Time': '',
            'In Person Interactions': 'QID15_5',
            'Understanding': '',
            'Contact ID': 'CONTACT_ID',
            'Project ID': 'PROJECT_ID',
            'Employee ID': '',
            'Vivint ID': 'TECH_ID',
            'Case ID': 'CASE_ID',
            'Service ID': 'SERVICE_ID',
            'Badge ID': 'TECH_BADGE_ID',
            'Task ID': '',
            'Work Order ID': 'WORK_ORDER_ID',
            'Survey ID': 'SV_1Y4igxkM532zQod',
            'Vivint ID 2': '',
            'Urgency': '',
            'Personal Effort': 'Q15',
            'Account Center Ease': '',
            'Savings': '',
            'May Contact': 'QID6',
            'Wants to be Contacted': 'QID13',
        }
    },
    'PIPSM': {
        'NPS': {
            'Response ID': 'ResponseID',
            'NPS Type': 'NPS',
            'NPS Score': ['QID1'],
            'NPS Comments': [
                'QID2',
                'QID7',
                'QID8'
            ]
        },
        'ANPS': {
            'Response ID': 'ResponseID',
            'NPS': 'ANPS',
            'NPS Score': ['QID3'],
            'NPS Comments': [
                'QID4',
                'QID11',
                'QID10'
            ]
        },
        'Response ID': {
            'Response ID': 'ResponseID',
            'Survey Type': '210 - Pre-Install Process',
            'Survey Finished': 'EndDate',
            'Communication': 'QID15_1',
            'Quality of Work': 'QID15_6',
            'Process Time': 'QID15_2',
            'In Person Interactions': 'QID15_5',
            'Understanding': 'QID15_3',
            'Contact ID': 'ContactId',
            'Project ID': 'ProjectId',
            'Employee ID': '',
            'Vivint ID': '',
            'Case ID': '',
            'Service ID': 'ServiceId',
            'Badge ID': '',
            'Task ID': '',
            'Work Order ID': '',
            'Survey ID': 'SV_3yNCgLrPE6LV3mJ',
            'Vivint ID 2': '',
            'Urgency': '',
            'Personal Effort': '',
            'Account Center Ease': '',
            'Savings': '',
            'May Contact': 'QID6',
            'Wants to be Contacted': 'QID13',
        }
    },
    'PTO45': {
        'NPS': {
            'Response ID': 'ResponseID',
            'NPS Type': 'NPS',
            'NPS Score': ['QID1'],
            'NPS Comments': [
                'QID2',
                'QID7',
                'QID8'
            ]
        },
        'Response ID': {
            'Response ID': 'ResponseID',
            'Survey Type': '510 - Billing',
            'Survey Finished': 'EndDate',
            'Communication': 'QID15_1',
            'Quality of Work': 'QID15_6',
            'Process Time': 'QID15_2',
            'In Person Interactions': '',
            'Understanding': 'QID15_3',
            'Contact ID': 'Contact ID',
            'Project ID': 'Project ID',
            'Employee ID': '',
            'Vivint ID': '',
            'Case ID': '',
            'Task ID': '',
            'Work Order ID': '',
            'Service ID': 'Service ID',
            'Survey ID': 'SV_e5wEuDcHC9IIKHj',
            'Vivint ID 2': '',
            'Urgency': '',
            'Personal Effort': '',
            'Account Center Ease': 'QID15_7',
            'Savings': '',
            'May Contact': 'QID6',
            'Wants to be Contacted': 'QID13',
        }
    },
    'PTO': {
        'NPS': {
            'Response ID': 'ResponseID',
            'NPS Type': 'NPS',
            'NPS Score': ['QID1'],
            'NPS Comments': [
                'QID2',
                'QID7',
                'QID8'
            ]
        },
        'ANPS': {
            'Response ID': 'ResponseID',
            'NPS': 'ANPS',
            'NPS Score': ['QID3'],
            'NPS Comments': [
                'QID4',
                'QID11',
                'QID10'
            ]
        },
        'Response ID': {
            'Response ID': 'ResponseID',
            'Survey Type': '500 - PTO',
            'Survey Finished': 'EndDate',
            'Communication': 'QID15_1',
            'Quality of Work': 'QID15_6',
            'Process Time': 'QID15_2',
            'In Person Interactions': 'QID15_5',
            'Understanding': 'QID15_3',
            'Contact ID': 'Contact ID',
            'Project ID': 'Project ID',
            'Employee ID': '',
            'Vivint ID': '',
            'Case ID': '',
            'Service ID': 'Service ID',
            'Badge ID': '',
            'Task ID': '',
            'Work Order ID': '',
            'Survey ID': 'SV_29sKA97g0oEIlkF',
            'Vivint ID 2': '',
            'Urgency': '',
            'Personal Effort': '',
            'Account Center Ease': '',
            'Savings': '',
            'May Contact': 'QID6',
            'Wants to be Contacted': 'QID13',
        }
    },
    'REL': {
        'NPS': {
            'Response ID': 'ResponseID',
            'NPS Type': 'NPS',
            'NPS Score': ['Q1'],
            'NPS Comments': [
                'Q2',
                'Q6',
                'Q7'
            ]
        },
        'Response ID': {
            'Response ID': 'ResponseID',
            'Survey Type': '600 - Relational',
            'Survey Finished': 'EndDate',
            'Communication': 'Q14_1',
            'Quality of Work': 'Q14_6',
            'Process Time': '',
            'In Person Interactions': 'Q14_5',
            'Understanding': '',
            'Contact ID': 'Contact ID',
            'Project ID': 'Project ID',
            'Employee ID': '',
            'Vivint ID': '',
            'Case ID': '',
            'Service ID': 'Service ID',
            'Badge ID': '',
            'Task ID': '',
            'Work Order ID': '',
            'Survey ID': 'SV_bNp2Xm2rZVtWab3',
            'Vivint ID 2': '',
            'Urgency': '',
            'Personal Effort': '',
            'Account Center Ease': '',
            'Savings': 'Q14_7',
            'May Contact': '',
            'Wants to be Contacted': 'Q12',
        }
    },
    'SSC': {
        'NPS': {
            'Response ID': 'ResponseID',
            'NPS Type': 'NPS',
            'NPS Score': ['QID1'],
            'NPS Comments': [
                'QID2',
                'QID7',
                'QID8'
            ]
        },
        'ANPS': {
            'Response ID': 'ResponseID',
            'NPS': 'ANPS',
            'NPS Score': ['QID3'],
            'NPS Comments': [
                'QID4',
                'QID11',
                'QID10'
            ]
        },
        'Response ID': {
            'Response ID': 'ResponseID',
            'Survey Type': '200 - Site Survey Complete',
            'Survey Finished': 'EndDate',
            'Communication': 'QID15_1',
            'Quality of Work': 'QID15_6',
            'Process Time': 'QID15_2',
            'In Person Interactions': 'QID15_5',
            'Understanding': 'QID15_3',
            'Contact ID': 'Contact ID',
            'Project ID': 'Project ID',
            'Employee ID': '',
            'Vivint ID': 'Employee ID',
            'Case ID': '',
            'Service ID': 'Service ID',
            'Badge ID': '',
            'Task ID': '',
            'Work Order ID': '',
            'Survey ID': 'SV_8HyPxkVN38jXTkF',
            'Vivint ID 2': '',
            'Urgency': '',
            'Personal Effort': '',
            'Account Center Ease': '',
            'Savings': '',
            'May Contact': 'QID6',
            'Wants to be Contacted': 'QID13',
        }
    },
    'UCSM': {
        'NPS': {
            'Response ID': 'ResponseID',
            'NPS Type': 'NPS',
            'NPS Score': ['QID1'],
            'NPS Comments': [
                'QID2',
                'QID7',
                'QID8'
            ]
        },
        'ANPS': {
            'Response ID': 'ResponseID',
            'NPS': 'ANPS',
            'NPS Score': ['QID3'],
            'NPS Comments': [
                'QID4',
                'QID11',
                'QID10'
            ]
        },
        'Response ID': {
            'Response ID': 'ResponseID',
            'Survey Type': '100 - Sales Experience',
            'Survey Finished': 'EndDate',
            'Communication': '',
            'Quality of Work': '',
            'Process Time': '',
            'In Person Interactions': '',
            'Understanding': '',
            'Contact ID': 'Contact ID',
            'Project ID': 'Project ID',
            'Employee ID': '',
            'Vivint ID': 'Employee ID',
            'Case ID': '',
            'Service ID': 'Service ID',
            'Badge ID': '',
            'Task ID': '',
            'Work Order ID': '',
            'Survey ID': 'SV_ctHi4PCGC2ZraS1',
            'Vivint ID 2': '',
            'Urgency': '',
            'Personal Effort': '',
            'Account Center Ease': '',
            'Savings': '',
            'May Contact': 'QID6',
            'Wants to be Contacted': 'QID13',
        }
    },
    'RTSOM': {
        'NPS': {
            'Response ID': 'ResponseID',
            'NPS Type': 'NPS',
            'NPS Score': ['Q8'],
        },
        'ANPS': {
            'Response ID': 'ResponseID',
            'NPS': 'ANPS',
            'NPS Score': ['Q5'],
        },
        'Response ID': {
            'Response ID': 'ResponseID',
            'Survey Type': 'RTS - Field Feedback',
            'Survey Finished': 'EndDate',
            'Communication': '',
            'Quality of Work': '',
            'Process Time': '',
            'In Person Interactions': '',
            'Understanding': '',
            'Contact ID': '',
            'Project ID': '',
            'Employee ID': '',
            'Vivint ID': '',
            'Case ID': '',
            'Service ID': '',
            'Badge ID': 'Badge ID',
            'Task ID': '',
            'Work Order ID': '',
            'Survey ID': 'SV_8eUFJC9DEyzqdQF',
            'Vivint ID 2': '',
            'Urgency': '',
            'Personal Effort': '',
            'Account Center Ease': '',
            'Savings': '',
            'May Contact': '',
            'Wants to be Contacted': '',
            'Job Profile': 'Job Profile'
        }
    },
    'LD': {
        'NPS': {
            'Response ID': 'ResponseID',
            'NPS Type': 'NPS',
            'NPS Score': ['QID1'],
            'NPS Comments': [
                'QID2',
                'QID7',
                'QID8'
            ]
        },
        'ANPS': {
            'Response ID': 'ResponseID',
            'NPS': 'ANPS',
            'NPS Score': ['QID3'],
            'NPS Comments': [
                'QID4',
                'QID11',
                'QID10'
            ]
        },
        'Response ID': {
            'Response ID': 'ResponseID',
            'Survey Type': '560 - LeaseDimensions',
            'Survey Finished': 'EndDate',
            'Communication': '',
            'Quality of Work': '',
            'Process Time': '',
            'In Person Interactions': '',
            'Understanding': '',
            'Contact ID': 'Contact ID',
            'Project ID': 'Project ID',
            'Employee ID': '',
            'Vivint ID': '',
            'Case ID': '',
            'Service ID': 'Service ID',
            'Badge ID': 'Badge ID',
            'Task ID': '',
            'Work Order ID': '',
            'Survey ID': 'SV_4YKHh79nXj5PuUl',
            'Vivint ID 2': '',
            'Urgency': '',
            'Personal Effort': '',
            'Account Center Ease': '',
            'Savings': '',
            'May Contact': '',
            'Wants to be Contacted': '',
            'Job Profile': 'Job Profile',
            'LD Username': 'LD User',
            'Issue Resolved': 'QID16'
        }
    },
}


def process_survey(survey_key, data):
    survey_info = type_dict[survey_key]

    header = data[0]
    del data[0:3]

    for row in data:
        if row[header.index('RecipientEmail')] != '':
            response_id = row[header.index(survey_info['NPS']['Response ID'])]
            survey_type = survey_info['Response ID']['Survey Type']
            survey_id = survey_info['Response ID']['Survey ID']
            survey_finished = row[header.index(survey_info['Response ID']['Survey Finished'])]
            survey_finished = parse(survey_finished)
            try:
                communication = row[header.index(survey_info['Response ID']['Communication'])]
            except:
                communication = ''
            try:
                qow = row[header.index(survey_info['Response ID']['Quality of Work'])]
            except:
                qow = ''
            try:
                process_time = row[header.index(survey_info['Response ID']['Process Time'])]
            except:
                process_time = ''
            try:
                ipi = row[header.index(survey_info['Response ID']['In Person Interactions'])]
            except:
                ipi = ''
            try:
                understanding = row[header.index(survey_info['Response ID']['Understanding'])]
            except:
                understanding = ''
            try:
                contact_id = row[header.index(survey_info['Response ID']['Contact ID'])]
            except:
                contact_id = ''
            try:
                project_id = row[header.index(survey_info['Response ID']['Project ID'])]
            except:
                project_id = ''
            try:
                employee_id = row[header.index(survey_info['Response ID']['Employee ID'])]
            except:
                employee_id = ''
            try:
                vivint_id = row[header.index(survey_info['Response ID']['Vivint ID'])]
            except:
                vivint_id = ''
            try:
                vivint_id_2 = row[header.index(survey_info['Response ID']['Vivint ID 2'])]
            except:
                vivint_id_2 = ''
            try:
                service_id = row[header.index(survey_info['Response ID']['Service ID'])]
            except:
                service_id = ''
            try:
                case_id = row[header.index(survey_info['Response ID']['Case ID'])]
            except:
                case_id = ''
            try:
                work_order_id = row[header.index(survey_info['Response ID']['Work Order ID'])]
            except:
                work_order_id = ''
            try:
                task_id = row[header.index(survey_info['Response ID']['Task ID'])]
            except:
                task_id = ''
            try:
                urgency = row[header.index(survey_info['Response ID']['Urgency'])]
            except:
                urgency = ''
            try:
                personal_effort = row[header.index(survey_info['Response ID']['Personal Effort'])]
            except:
                personal_effort = ''
            try:
                account_center_ease = row[header.index(survey_info['Response ID']['Account Center Ease'])]
            except:
                account_center_ease = ''
            try:
                savings = row[header.index(survey_info['Response ID']['Savings'])]
            except:
                savings = ''
            try:
                badge_id = row[header.index(survey_info['Response ID']['Badge ID'])]
            except:
                badge_id = ''
            try:
                may_contact = row[header.index(survey_info['Response ID']['May Contact'])]
            except:
                may_contact = ''
            try:
                wants_contact = row[header.index(survey_info['Response ID']['Wants to be Contacted'])]
            except:
                wants_contact = ''
            try:
                job_profile = row[header.index(survey_info['Response ID']['Job Profile'])]
            except:
                job_profile = ''
            try:
                ld_user_name = row[header.index(survey_info['Response ID']['LD Username'])]
            except:
                ld_user_name = ''
            try:
                issue_resolved = row[header.index(survey_info['Response ID']['Issue Resolved'])]
            except:
                issue_resolved = ''

            if job_profile == '':
                job_profile = 'OM'

            if may_contact == '2':
                may_contact = '0'

            if wants_contact == '3':
                wants_contact = '0'

            nps_score = max(row[header.index(comment)] for comment in survey_info['NPS']['NPS Score'])
            if 'NPS Comments' in survey_info['NPS'].keys():
                nps_comment = [row[header.index(comment)] for comment in survey_info['NPS']['NPS Comments']]
            else:
                nps_comment = ''
            try:
                nps_comment = list(filter(None, nps_comment))[0]
            except:
                nps_comment = ''
            nps_comment = nps_comment.replace('\\\\', '\n')
            if 'ANPS' in survey_info.keys():
                anps_score = max(row[header.index(comment)] for comment in survey_info['ANPS']['NPS Score'])
                if 'NPS Comments' in survey_info['ANPS'].keys():
                    anps_comment = [row[header.index(comment)] for comment in survey_info['ANPS']['NPS Comments']]
                else:
                    anps_comment = ''
                try:
                    anps_comment = list(filter(None, anps_comment))[0]
                except:
                    anps_comment = ''
                anps_comment = anps_comment.replace('\\\\', '\n')
            else:
                anps_score = ''
                anps_comment = ''

            if 'ANPS2' in survey_info.keys():
                anps2_score = max(row[header.index(comment)] for comment in survey_info['ANPS2']['NPS Score'])
                anps2_comment = [row[header.index(comment)] for comment in survey_info['ANPS2']['NPS Comments']]
                try:
                    anps2_comment = list(filter(None, anps2_comment))[0].replace('\\\\', '\n')
                except:
                    anps2_comment = ''

            else:
                anps2_score = ''
                anps2_comment = ''

            public_data_warehouse_row = [
                response_id,
                contact_id,
                survey_id,
                project_id,
                employee_id,
                survey_type,
                nps_score,
                anps_score,
                anps2_score,
                nps_comment,
                anps_comment,
                anps2_comment,
                survey_finished,
                communication,
                qow,
                process_time,
                ipi,
                understanding,
                urgency,
                personal_effort,
                account_center_ease,
                savings,
                vivint_id,
                vivint_id_2,
                case_id,
                badge_id,
                service_id,
                work_order_id,
                task_id,
                may_contact,
                wants_contact,
                job_profile,
                ld_user_name,
                issue_resolved,
            ]

            public_data_warehouse.append(public_data_warehouse_row)


def qualtrics_processor():
    for filename in os.listdir(os.path.join(find_main_dir(), 'data\\qualtrics_jsons')):
        if '.json' in filename:
            fn = os.path.join(find_main_dir(), 'data\\qualtrics_jsons\\' + filename)
            key = filename.replace('.json', '')
            with open(fn, 'r') as outfile:
                data = json.load(outfile)
            data = data[key]

            if key == 'NPS - Home Damage':
                process_survey('AFHD', data)
            elif key == 'NPS - Customer Service':
                process_survey('CS', data)
            elif key == 'NPS - CSM':
                process_survey('CSM', data)
            elif key == 'NPS - Executive Resolutions':
                process_survey('ERCC', data)
            elif key == 'NPS - FIN':
                process_survey('FC', data)
            elif key == 'NPS - Home Upgrade':
                process_survey('HU', data)
            elif key == 'NPS - Install':
                process_survey('ICO', data)
            elif key == 'NPS - Intro Call':
                process_survey('ICA', data)
            elif key == 'NPS - Install + 45 (Retired)':
                process_survey('PIP', data)
            elif key == 'NPS - Post-PTO Field Service':
                process_survey('PPWO', data)
            elif key == 'NPS - Site Survey + 45 (Retired)':
                process_survey('PIPSM', data)
            elif key == 'NPS - PTO':
                process_survey('PTO', data)
            elif key == 'NPS - PTO + 45':
                process_survey('PTO45', data)
            elif key == 'NPS - Relational':
                process_survey('REL', data)
            elif key == 'NPS - Site Survey':
                process_survey('SSC', data)
            elif key == 'NPS - Sales Experience':
                process_survey('UCSM', data)
            elif key == 'RTS-OM Feedback Loop':
                process_survey('RTSOM', data)
            elif key == 'NPS - LD':
                process_survey('LD', data)

    return {
        'public': public_data_warehouse,
        # 'private': private_data_warehouse
    }


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
    header = data[0]

    for i, row in enumerate(data):
        for j, cell in enumerate(row):
            s = cell
            if j == header.index('NPS_SCORE') \
                    or j == header.index('ANPS_SCORE') \
                    or j == header.index('COMMUNICATION') \
                    or j == header.index('QUALITY_OF_WORK') \
                    or j == header.index('PROCESS_TIME') \
                    or j == header.index('IN_PERSON_INTERACTIONS') \
                    or j == header.index('UNDERSTANDING'):
                if is_number(s):
                    try:
                        data[i][j] = int(s)
                    except:
                        pass

            if isinstance(s, str):
                data[i][j] = remove_non_ascii_2(s)

    return data
