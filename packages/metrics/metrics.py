from .. import *


# %% Task Metrics
class TaskMetrics:
    def __init__(self, task):
        self.task = task
        self.id = self.task.id
        self.run_type = self.task.operational
        self.input_time = self.task.input_time
        self.output_time = self.task.output_time
        self.upload_time = self.task.upload_time
        self.task_completion_time = 0
        self.upload_line = [[self.id, self.task_completion_time, self.input_time,
                             self.output_time, self.upload_time, 0,
                             self.run_type, dt.datetime.now()
                             ]]
        self.remove_none()
        self.table_name = 'D_POST_INSTALL.T_AUTO_METRICS'
        self.set_task_completion_time()

    def remove_none(self):
        self.upload_line[0] = ['' if x is None else x for x in self.upload_line[0]]

    def set_task_completion_time(self):
        if isinstance(self.input_time, int):
            self.task_completion_time += self.input_time
        if isinstance(self.output_time, int):
            self.task_completion_time += self.output_time
        if isinstance(self.upload_time, int):
            self.task_completion_time += self.upload_time

    def submit_task_time(self):
        self.input_time = self.task.input_time
        self.output_time = self.task.output_time
        self.upload_time = self.task.upload_time
        self.set_task_completion_time()
        self.upload_line = [[self.id, self.task_completion_time, self.input_time,
                             self.output_time, self.upload_time, 0,
                             self.run_type, dt.datetime.now()
                             ]]
        self.task.dw.insert_into_table(self.table_name, self.upload_line)
