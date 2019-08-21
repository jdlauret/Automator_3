from datetime import datetime as dt


class TaskTable:
    def __init__(self, task):
        self.task = task
        self.id = task.id
        self.name = task.name
        self.db_table = task.db_table
        self.dw = task.dw

    def update_last_run(self):
        ts = dt.now()
        self.task.MetaData = self.task.MetaData._replace(last_run=ts)

    def update_last_attempt(self):
        ts = dt.now()
        self.task.MetaData = self.task.MetaData._replace(last_attempt=ts)

    def update_meta_data(self):
        query = '''UPDATE {table}\n
                SET LAST_ATTEMPT = TO_TIMESTAMP('{last_attempt}', 'YYYY-MM-DD hh:mi:ss'),
                LAST_RUN = TO_TIMESTAMP('{last_run}', 'YYYY-MM-DD hh:mi:ss')
                WHERE ID = {id}'''.format(table=self.db_table, id=self.task.id,
                                          last_attempt=self.task.MetaData.last_attempt.strftime('%Y-%m-%d %H:%M:%S'),
                                          last_run=self.task.MetaData.last_run.strftime('%Y-%m-%d %H:%M:%S'))
        self.dw.execute_sql_command(query)

    def update_run_requested(self):
        query = '''UPDATE {table}\n
        SET RUN_REQUESTED = \'FALSE\'\n
        WHERE ID = {id}'''.format(table=self.db_table, id=self.task.id)
        self.dw.execute_sql_command(query)

    def disable_task(self):
        query = '''UPDATE {table}\n
        SET OPERATIONAL = \'Disabled\'\n
        WHERE ID = {id}'''.format(table=self.db_table, id=self.task.id)
        self.dw.execute_sql_command(query)

    def pause_task(self):
        query = '''UPDATE {table}\n
        SET OPERATIONAL = \'Paused\'\n
        WHERE ID = {id}'''.format(table=self.db_table, id=self.task.id)
        self.dw.execute_sql_command(query)

    def resume_task(self):
        if (self.task.Logger.paused or self.task.Logger.disabled) \
                and self.task.task_complete:
            if self.task.db_table is None or self.id is None:
                print(self.task.task_name, '\ndb_table:', self.db_table, '\nid:', self.id)
            query = '''UPDATE {table}\n
            SET OPERATIONAL = \'Operational\'\n
            WHERE ID = {id}'''.format(table=self.db_table, id=self.id)
            self.dw.execute_sql_command(query)
