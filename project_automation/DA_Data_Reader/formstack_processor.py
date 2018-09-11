import datetime as dt
import threading
import os
import re
import sys
from models import SnowFlakeDW, SnowflakeConsole, GSheets


def find_main_dir():
    """
    Returns the Directory of the main running file
    """
    if getattr(sys, 'frozen', False):
        # The application is frozen
        return os.path.dirname(sys.executable)

    else:
        # The application is not frozen
        # Change this bit to match where you store your data files:
        return os.path.dirname(os.path.realpath(__file__))


def find_foreman():
    file_data = """
    SELECT 
    p.PROJECT_ID
    ,to_number(regexp_replace(p.service_number, '[^[:digit:]]', '')) AS AR
    ,p.project_name AS PROJECT
    ,NVL(p.roc_name, p.sales_office) AS OFFICE
    ,DATE_TRUNC(day, p.installation_complete) AS INSTALL_COMPLETE
    ,CASE 
    WHEN wo.technician_name IS NOT NULL 
    THEN wo.technician_name || ' (' || wo.technician_badge_id || ')' 
    END AS EMPLOYEE,
    MWC.SUPERVISOR_NAME_1 SUPERVISOR,
    MWC.BUSINESS_TITLE JOB_TITLE,
    MWC.COST_CENTER DEPARTMENT
    FROM   rpt.t_project p
    LEFT   JOIN rpt.T_PROJECT_EXT cdm
    ON     cdm.project_id = p.project_id
    LEFT   JOIN rpt.t_workorder wo
    ON     wo.workorder_id = cdm.install_first_work_order_id
    LEFT JOIN HR.T_EMPLOYEE MWC 
    ON wo.TECHNICIAN_BADGE_ID = MWC.BADGE_ID
    WHERE p.installation_complete > '01-JAN-16'
    """

    file_data = re.sub(' +', ' ', file_data)
    file_data = file_data.replace(';', '')
    dw = SnowflakeConsole(db)
    dw.execute_query(file_data)
    return dw.query_with_header


class EscFormstackProcessor:

    def __init__(self, data, header):
        self.data = data
        self.header = header
        self.formatted_data = data
        self.qa_info = [
            [
                'Date',
                'Review Type',
                'Auditor',
                'AR',
                'Foreman',
                'Job Title',
                'Supervisor',
                'Install Complete',
                'Office',
            ]
        ]
        self.question_results = [
            [
                'AR',
                'Survey Type',
                'Question',
                'Result',
                'Fail',
                'Coach',
                'N/A',
            ]
        ]
        self.results_header = self.question_results[0]

        self.first_question_col = self.header.index('Other') + 1
        self.last_question_col = [i for i, n in enumerate(self.header) if n == 'Browser'][-1]

        self.questions_header = header[self.first_question_col:self.last_question_col]
        self.column_dict = {
            'AR': 'AR/Service number',
            'Date': 'Time',
            'Survey Type': 'Type of review',
            'Upgrade': 'Electrical upgrade',
            'Other': 'Other',
            'Auditor Name': 'Person performing audit'
        }

    def question_processing(self):
        print()
        for j, line in enumerate(self.data):
            new_line = []
            for i, item in enumerate(line):
                if line[0] != '':
                    if self.header[i] == 'Time' and i < self.last_question_col:
                        try:
                            date = dt.datetime.strptime(item, "%b %d, %Y %H:%M:%S").strftime("%m/%d/%Y")
                        except:
                            date = dt.datetime.strptime(item, "%b %d, %Y %H:%M:%S").strftime("%m/%d/%Y")
                        new_line.append(date)
                    elif self.first_question_col <= i <= self.last_question_col:
                        cell = str(item)
                        if cell in ['Pass', 'Fail', 'Coach', '']:
                            if cell == '':
                                new_line.append('N/A')
                            else:
                                new_line.append(cell)
                    else:
                        new_line.append(item)
                else:
                    break
            self.formatted_data.append(new_line)

    def account_info(self):
        self.foreman = ''
        self.install_complete = ''
        self.job_title = ''
        self.department = ''
        self.supervisor = ''
        self.office = ''
        for account in foreman_db:
            if int(self.ar) in account or str(self.ar) in account:
                self.foreman = account[foreman_header.index('EMPLOYEE')]
                self.install_complete = account[foreman_header.index('INSTALL_COMPLETE')]
                self.job_title = account[foreman_header.index('JOB_TITLE')]
                self.department = account[foreman_header.index('DEPARTMENT')]
                self.supervisor = account[foreman_header.index('SUPERVISOR')]
                self.office = account[foreman_header.index('OFFICE')]
                break

    def create_qa_info(self):
        print()

        start = dt.datetime.now()
        for i, row in enumerate(self.formatted_data):
            self.ar = row[self.header.index(self.column_dict['AR'])]
            qa_submitted = row[self.header.index(self.column_dict['Date'])]
            survey_type = row[self.header.index(self.column_dict['Survey Type'])]
            auditor_name = row[self.header.index(self.column_dict['Auditor Name'])]

            self.account_info()

            new_row = [
                qa_submitted,
                survey_type,
                auditor_name,
                self.ar,
                self.foreman,
                self.job_title,
                self.supervisor,
                self.install_complete,
                self.office,
            ]
            self.qa_info.append(new_row)
        end = (dt.datetime.now() - start).seconds
        print('QA info table build in {0} seconds'.format(end))
        print()

    def create_question_results(self):
        start = dt.datetime.now()
        for j, line in enumerate(self.formatted_data):
            if line != self.header:
                ar = line[self.header.index(self.column_dict['AR'])]
                results = line[self.first_question_col:self.last_question_col]
                survey_type = line[self.header.index(self.column_dict['Survey Type'])]

                for i, result in enumerate(results):
                    question = self.questions_header[i]
                    if result != 'Pass' \
                            and 'additional comments' not in question.lower() \
                            and 'image' not in question.lower() \
                            and 'truck roll' not in question.lower() \
                            and 'proof of fix' not in question.lower() \
                            and (question[0].isalpha()
                                 and question[1].isdigit()
                                 and question[2].isdigit()):
                        if result == '':
                            result = 'N/A'
                        question_line = [ar, survey_type, question, result, None, None, None]

                        if result == 'Fail':
                            question_line[self.results_header.index('Fail')] = 1
                        elif result == 'Coach':
                            question_line[self.results_header.index('Coach')] = 1
                        elif result == 'N/A':
                            question_line[self.results_header.index('N/A')] = 1

                        self.question_results.append(question_line)

        end = (dt.datetime.now() - start).seconds
        print('QA Question Results table built in {0} seconds'.format(end))
        print()


class QaFormstackProcessor:

    def __init__(self, data, header, formstack):
        self.data = data
        self.header = header
        self.formstack = formstack
        self.formatted_data = list()
        self.qa_info = [
            [
                'Date',
                'Team',
                'Auditor Name',
                'AR',
                'Foreman',
                'Job Title',
                'Department',
                'Supervisor',
                'Installation Complete',
                'Action Needed Category',
                'DA: Complete',
                'DA: Action Taken',
                'DA: Sent to HR',
                'DA: HR Approved',
                'DA: Sent To Manager',
                'DA: Comments',
                'DA: In Workday',
                'Duration',
                'Inspection Type',
                'Office',
                'Formstack Version',
            ]
        ]
        self.question_results = [
            [
                'AR',
                'Question',
                'Result',
                'Level 1',
                'Level 2',
                'Level 3',
                'Level 4',
                'Level 5',
                'N/A',
                'Survey Type'
            ]
        ]
        self.results_header = self.question_results[0]

        if self.formstack == 'old':
            self.first_question_col = self.header.index('Roof Access') + 1
            self.last_question_col = self.header.index('Regional Recommendation') - 1
            self.version = '1'
            self.additional_comment_count = 0
            self.additional_comment_list = {
                0: "Array",
                1: "Conduits and Jboxes",
                2: "Ground Level Equipment",
                3: "MSP/MDP Interconnection"
            }
            for i, item in enumerate(header):
                if 'Additional Comments' in item:
                    self.additional_comment_count += 1
                elif self.first_question_col <= i <= self.last_question_col and 'Additional Comments' not in item:
                    trim_start = item.index(' (')
                    trimmed_item = item[:trim_start]
                    new_item = self.additional_comment_list[self.additional_comment_count] + ' ' + trimmed_item
                    self.header[i] = new_item

        elif self.formstack == 'new':
            self.first_question_col = self.header.index('Percent attic score')
            self.last_question_col = self.header.index('Browser') - 1
            self.version = '2'

        self.questions_header = header[self.first_question_col:self.last_question_col]
        self.column_dict = {
            'old': {
                'AR': 'AR',
                'Date': 'Date',
                'Survey Type': 'Team',
                'Auditor Name': 'Auditor Name',
                'Inspection Type': None,
                'Duration': None,
            },
            'new': {
                'AR': 'AR',
                'Date': 'Date',
                'Survey Type': 'Type of inspection being performed',
                'Auditor Name': 'Technician performing audit',
                'Inspection Type': 'Team',
                'Duration': 'Duration',
            },
        }
        self.question_processing()

    def question_processing(self):
        print()
        print('Formatting Questions for {0} formstack data'.format(self.formstack.title()))
        for j, line in enumerate(self.data):
            new_line = []
            for i, item in enumerate(line):
                if line[0] != '':
                    if self.header[i] == 'Date' and i < self.last_question_col:
                        try:
                            date = dt.datetime.strptime(item, "%B %d, %Y").strftime("%m/%d/%Y")
                        except:
                            date = dt.datetime.strptime(item, "%b %d, %Y").strftime("%m/%d/%Y")
                        new_line.append(date)
                    elif self.first_question_col <= i <= self.last_question_col:
                        cell = str(item)
                        formatted_cell = ''
                        if 'Pass' in cell or cell == '-0.00001':
                            formatted_cell = 'Pass'
                        elif 'Fail' in cell:
                            if '1' in cell:
                                formatted_cell = 1
                            elif '2' in cell:
                                formatted_cell = 2
                            elif '3' in cell:
                                formatted_cell = 3
                            elif '4' in cell:
                                formatted_cell = 4
                            elif '5' in cell:
                                formatted_cell = 5
                        elif 'N/A' in cell or cell == '':
                            formatted_cell = 'N/A'
                        elif cell == '-0.001':
                            formatted_cell = 1
                        elif cell == '-1':
                            formatted_cell = 2
                        elif cell == '-2':
                            formatted_cell = 3
                        elif cell == '-4':
                            formatted_cell = 4
                        elif cell == '-8':
                            formatted_cell = 5
                        elif cell == '-0.0001':
                            formatted_cell = 'Coach'
                        else:
                            formatted_cell = 'N/A'

                        new_line.append(formatted_cell)
                    else:
                        new_line.append(item)
                else:
                    break
            self.formatted_data.append(new_line)

    def account_info(self):
        self.foreman = ''
        self.install_complete = ''
        self.job_title = ''
        self.department = ''
        self.supervisor = ''
        self.office = ''
        for account in foreman_db:
            if int(self.ar) == account[foreman_header.index('AR')] \
                    or str(self.ar) == account[foreman_header.index('AR')]:
                self.foreman = account[foreman_header.index('EMPLOYEE')]
                self.install_complete = account[foreman_header.index('INSTALL_COMPLETE')]
                self.job_title = account[foreman_header.index('JOB_TITLE')]
                self.department = account[foreman_header.index('DEPARTMENT')]
                self.supervisor = account[foreman_header.index('SUPERVISOR')]
                self.office = account[foreman_header.index('OFFICE')]
                break

    def da_info(self):
        self.da_complete = ''
        self.da_action_taken = ''
        self.da_sent_to_hr = ''
        self.da_hr_approved = ''
        self.da_sent_to_manager = ''
        self.da_comments = ''
        self.da_in_workday = ''
        self.da_action_needed = ''
        for da_row in db_qa_data:
            if int(self.ar) in da_row \
                    or str(self.ar) in da_row:
                self.da_complete = da_row[db_qa_header.index('Complete')]
                self.da_action_taken = da_row[db_qa_header.index('Form of Action Taken')]
                self.da_sent_to_hr = da_row[db_qa_header.index('Sent to HR')]
                self.da_hr_approved = da_row[db_qa_header.index('HR Approved')]
                self.da_sent_to_manager = da_row[db_qa_header.index('Sent to Manager')]
                self.da_comments = da_row[db_qa_header.index('Comments')]
                self.da_in_workday = da_row[db_qa_header.index('In Workday')]
                self.da_action_needed = da_row[db_qa_header.index('Action Needed Category')]
                break

    def create_qa_info(self):
        print()
        print('Creating QA Info Table from {0} Formstack data'.format(self.formstack.title()))

        start = dt.datetime.now()
        for i, row in enumerate(self.formatted_data):
            self.ar = row[self.header.index(self.column_dict[self.formstack]['AR'])]
            qa_submitted = row[self.header.index(self.column_dict[self.formstack]['Date'])]
            survey_type = row[self.header.index(self.column_dict[self.formstack]['Survey Type'])]
            auditor_name = row[self.header.index(self.column_dict[self.formstack]['Auditor Name'])]
            if self.column_dict[self.formstack]['Inspection Type'] is not None:
                inspection_type = row[self.header.index(self.column_dict[self.formstack]['Inspection Type'])]
            else:
                inspection_type = ''
            if self.column_dict[self.formstack]['Duration'] is not None:
                # duration = row[self.header.index(self.column_dict[self.formstack]['Duration'])]
                duration = 0
            else:
                duration = 0

            p1 = threading.Thread(target=self.account_info)
            p2 = threading.Thread(target=self.da_info)

            p1.start()
            p2.start()

            p1.join()
            p2.join()

            new_row = [
                qa_submitted,
                survey_type,
                auditor_name,
                self.ar,
                self.foreman,
                self.job_title,
                self.department,
                self.supervisor,
                self.install_complete,
                self.da_action_needed,
                self.da_complete,
                self.da_action_taken,
                self.da_sent_to_hr,
                self.da_hr_approved,
                self.da_sent_to_manager,
                self.da_comments,
                self.da_in_workday,
                duration,
                inspection_type,
                self.office,
                self.version,
            ]
            self.qa_info.append(new_row)
        end = (dt.datetime.now() - start).seconds
        print('QA info table build in {0} seconds'.format(end))
        print()

    def create_question_results(self):
        print('Creating QA Question Results Table from {0} Formstack data'.format(self.formstack.title()))
        start = dt.datetime.now()
        for j, line in enumerate(self.formatted_data):
            if line != self.header:
                ar = line[self.header.index(self.column_dict[self.formstack]['AR'])]
                results = line[self.first_question_col:self.last_question_col]
                survey_type = line[self.header.index(self.column_dict[self.formstack]['Survey Type'])]

                for i, result in enumerate(results):
                    question = self.questions_header[i]
                    if result != 'Pass' \
                            and 'additional comments' not in question.lower() \
                            and 'image' not in question.lower() \
                            and ((question[0].isalpha()
                                  and question[1].isdigit()
                                  and question[2].isdigit()
                                  and self.formstack == 'new')
                                 or self.formstack == 'old'):

                        question_line = [ar, question, result, None, None, None, None, None, None, survey_type]

                        if result == 1:
                            question_line[self.results_header.index('Level 1')] = 1
                        elif result == 2:
                            question_line[self.results_header.index('Level 2')] = 1
                        elif result == 3:
                            question_line[self.results_header.index('Level 3')] = 1
                        elif result == 4:
                            question_line[self.results_header.index('Level 4')] = 1
                        elif result == 5:
                            question_line[self.results_header.index('Level 5')] = 1
                        elif result == 'N/A':
                            question_line[self.results_header.index('N/A')] = 1

                        self.question_results.append(question_line)

        end = (dt.datetime.now() - start).seconds
        print('QA Question Results table built in {0} seconds'.format(end))
        print()


def combine_lists(list1, list2, list2_header=True):
    if list2_header:
        del list2[0]
    return list1 + list2


def qa_data_handler():
    print('Initializing Formstack Processors')
    old_formstack = QaFormstackProcessor(old_formstack_data, old_formstack_header, 'old')
    new_formstack = QaFormstackProcessor(new_formstack_data, new_formstack_header, 'new')
    print()
    print('Initialization complete')

    old_formstack.create_qa_info()
    new_formstack.create_qa_info()

    old_formstack.create_question_results()
    new_formstack.create_question_results()

    qa_info = combine_lists(old_formstack.qa_info, new_formstack.qa_info)
    question_results = combine_lists(old_formstack.question_results, new_formstack.question_results)

    regional_list = [
        [
            row[db_validation_header.index('Office')],
            row[db_validation_header.index('Regional OM')]
        ] for row in db_validation_data if row[db_validation_header.index('Office')] != '']

    qa_info_table = 'D_POST_INSTALL.T_FS_QA_INFO'
    qa_questions_table = 'D_POST_INSTALL.T_FS_QUESTION_RESULTS'

    if update_tables:
        dw = SnowflakeConsole(db)
        dw.insert_into_table(qa_info_table, qa_info, overwrite=True, header_included=True)
        dw.insert_into_table(qa_questions_table, question_results, overwrite=True, header_included=True)


def esc_data_handler():
    esc_formstack_data = GSheets('1Zt2qTfiPleVn4zhXTQG_n6woNTvE_0G9pp-rmz6fOzI').get_sheet_data('Sheet1')
    esc_formstack_header = esc_formstack_data[0]
    del esc_formstack_data[0]

    esc_formstack = EscFormstackProcessor(esc_formstack_data, esc_formstack_header)
    esc_formstack.create_qa_info()
    esc_formstack.create_question_results()

    esc_info = esc_formstack.qa_info
    question_results = esc_formstack.question_results

    esc_info_table = 'D_POST_INSTALL.T_FS_ESC_INFO'
    esc_questions_table = 'D_POST_INSTALL.T_FS_ESC_QUESTION_RESULTS'

    if update_tables:
        dw = SnowflakeConsole(db)

        dw.insert_into_table(esc_info_table, esc_info, overwrite=True, header_included=True)

        dw.insert_into_table(esc_questions_table, question_results, overwrite=True, header_included=True)


if __name__ == '__main__':
    db = SnowFlakeDW()
    db.set_user('JDLAURET')
    db.open_connection()

    run_qa_data = True
    run_esc_data = True
    update_tables = True
    print('Getting Foreman Results')
    foreman_db = find_foreman()
    foreman_header = foreman_db[0]
    del foreman_db[0]

    if run_qa_data:
        print("Opening Formstack Data Sheets and DA Sheet")
        old_formstack_data = GSheets('1OyV1Z1OhInpV-25p6y953iX3EZZgMuHL3YNtDV9ZJuw').get_sheet_data('Sheet1')
        new_formstack_data = GSheets('185QE7xVUzWoNhRm_Ysz5Fld1f9-Nm0EaTyiUeP4TyCk').get_sheet_data('Sheet1')
        db_qa_data = GSheets('1aa1BS1UVUCKJJ12XqulmDaMr7oLjsLOf62kBn0mX7zc').get_sheet_data('Quality Data')
        db_validation_data = GSheets('1aa1BS1UVUCKJJ12XqulmDaMr7oLjsLOf62kBn0mX7zc').get_sheet_data("Validation Import")

        print('Sheets opened and values stored')

        old_formstack_header = old_formstack_data[0]
        new_formstack_header = new_formstack_data[0]
        db_qa_header = db_qa_data[1]
        db_validation_header = db_validation_data[3]

        del db_qa_data[0:1]
        del db_validation_data[0:3]

        if old_formstack_data[0] == old_formstack_header:
            del old_formstack_data[0]
        if new_formstack_data[0] == new_formstack_header:
            del new_formstack_data[0]
        qa_data_handler()

    if run_esc_data:
        esc_data_handler()

    db.close_connection()
