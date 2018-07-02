# -----------------------------------------------------------------------
# Schedule Loop for Automator-Luna
# Rio Branham: 2018-05-03
# -----------------------------------------------------------------------

# %% Setup

# import DataWarehous, Task
import sched
import time
import re
from calendar import Calendar as Cal
from datetime import datetime as dt, timedelta as td
from models import Task, DataWarehouse

# Declarations
day_dict = {
    'MONDAY': 0,
    'TUESDAY': 1,
    'WEDNESDAY': 2,
    'THURSDAY': 3,
    'FRIDAY': 4,
    'SATURDAY': 5,
    'SUNDAY': 6
}

month_num_dict = {
    'JANUARY': 1,
    'FEBRUARY': 2,
    'MARCH': 3,
    'APRIL': 4,
    'MAY': 5,
    'JUNE': 6,
    'JULY': 7,
    'AUGUST': 8,
    'SEPTEMBER': 9,
    'OCTOBER': 10,
    'NOVEMBER': 11,
    'DECEMBER': 12
}

# %% Time Functions


def dt_to_sec(datetime=None):
    if datetime is None:
        return time.time()
    else:
        return time.mktime(datetime.timetuple())


def next_hour():
    return dt.today().replace(minute=0, second=0) + td(hours=1)


def next_day():
    return dt.today().replace(hour=0, minute=0, second=0) + td(1)


def next_week():
    return (dt.today().replace(hour=0, minute=0, second=0)
            + td(7 - dt.today().weekday()))


def next_month():
    return (dt.today() + td(28)).replace(day=1, hour=0, minute=0, second=0)


# %% Schedule Functions


def hourly():
    s.enterabs(dt_to_sec(next_hour()), 1, hourly)
    dw = DataWarehouse('admin')
    sql = """
    SELECT *
    FROM rio_branham.t_auto_tasks tat
    WHERE tat.operational = 'Operational'
          AND tat.start_datetime <= SYSDATE
          AND tat.auto_recurrence = 'Hourly'
    """
    dw.query_results(sql)
    for task in dw.results:
        current_task = Task(task, dw.column_names)
        minutes = re.sub('[ ]?', '', current_task.task_data['minutes'])
        for run_time in minutes.split(','):
            s.enterabs(
                dt_to_sec(
                    dt.now().replace(
                        minute=int(run_time),
                        second=0
                    )
                ),
                current_task.task_data['priority'],
                current_task.run_task
            )


def daily():
    s.enterabs(dt_to_sec(next_day()), 1, daily)
    dw = DataWarehouse('admin')
    sql = """
    SELECT *
    FROM rio_branham.t_auto_tasks tat 
    WHERE tat.operational = 'Operational'
          AND tat.start_datetime <= SYSDATE
          AND tat.auto_recurrence = 'Daily'
    """
    dw.query_results(sql)
    for task in dw.results:
        current_task = Task(task, dw.column_names)
        times = re.sub('[ ]?', '', current_task.task_data['times'])
        for run_time in times.split(','):
            s.enterabs(
                dt_to_sec(
                    dt.now().replace(
                        hour=int(re.sub('^(\d+):.*', '\\1', run_time)),
                        minute=int(re.sub('.*:(\d+)', '\\1', run_time)),
                        second=0
                    )
                ),
                current_task.task_data['priority'],
                current_task.run_task
            )


def weekly():
    s.enterabs(dt_to_sec(next_week()), 1, weekly)
    dw = DataWarehouse('admin')
    sql = """
    SELECT *
    FROM rio_branham.t_auto_tasks tat 
    WHERE tat.operational = 'Operational'
          AND tat.start_datetime <= SYSDATE
          AND tat.auto_recurrence = 'Weekly'
    """
    dw.query_results(sql)
    for task in dw.results:
        current_task = Task(task, dw.column_names)
        days = re.sub('[ ]?', '', current_task.task_data['week_days'])
        times = re.sub('[ ]?', '', current_task.task_data['times'])
        cadence = current_task.task_data['weeks']
        start_week = int(
            current_task.task_data['start_datetime'].strftime('%W')
        )
        weeks_since = int(dt.now().strftime('%W')) - start_week
        if weeks_since % cadence:
            continue
        for day in days.split(','):
            for run_time in times.split(','):
                s.enterabs(
                    dt_to_sec(
                        dt.now().replace(
                            day=dt.now().day + day_dict[day.upper()],
                            hour=int(re.sub('^(\d+):.*', '\\1', run_time)),
                            minute=int(re.sub('.*:(\d+)', '\\1', run_time)),
                            second=0
                        )
                    ),
                    current_task.task_data['priority'],
                    current_task.run_task
                )


def monthly():
    s.enterabs(dt_to_sec(next_month()), 1, monthly)
    if dt.now().month in (4, 6, 9, 11):
        days_in_month = 30
    elif dt.now().month == 2:
        days_in_month = 28
    else:
        days_in_month = 31
    month_list = Cal().monthdayscalendar(dt.now().year, dt.now().month)
    first_mon = 0
    first_tue = 0
    first_wed = 0
    first_thu = 0
    first_fri = 0
    first_sat = 0
    first_sun = 0

    second_mon = 0
    second_tue = 0
    second_wed = 0
    second_thu = 0
    second_fri = 0
    second_sat = 0
    second_sun = 0

    third_mon = 0
    third_tue = 0
    third_wed = 0
    third_thu = 0
    third_fri = 0
    third_sat = 0
    third_sun = 0

    fourth_mon = 0
    fourth_tue = 0
    fourth_wed = 0
    fourth_thu = 0
    fourth_fri = 0
    fourth_sat = 0
    fourth_sun = 0

    last_mon = 0
    last_tue = 0
    last_wed = 0
    last_thu = 0
    last_fri = 0
    last_sat = 0
    last_sun = 0

    # First, Second, Third and Fourth Weekdays in Month
    for week in month_list:
        # Mondays
        if week[0] and not first_mon:
            first_mon = week[0]
        elif week[0] and first_mon and not second_mon:
            second_mon = week[0]
        elif week[0] and first_mon and second_mon and not third_mon:
            third_mon = week[0]
        elif (week[0] and first_mon and second_mon and third_mon and
              not fourth_mon):
            fourth_mon = week[0]
        # Tuesdays
        if week[1] and not first_tue:
            first_tue = week[1]
        elif week[1] and first_tue and not second_tue:
            second_tue = week[1]
        elif week[1] and first_tue and second_tue and not third_tue:
            third_tue = week[1]
        elif (week[1] and first_tue and second_tue and third_tue and
              not fourth_tue):
            fourth_tue = week[1]
        # Wednesdays
        if week[2] and not first_wed:
            first_wed = week[2]
        elif week[2] and first_wed and not second_wed:
            second_wed = week[2]
        elif week[2] and first_wed and second_wed and not third_wed:
            third_wed = week[2]
        elif (week[2] and first_wed and second_wed and third_wed and
              not fourth_wed):
            fourth_wed = week[2]
        # Thursdays
        if week[3] and not first_thu:
            first_thu = week[3]
        elif week[3] and first_thu and not second_thu:
            second_thu = week[3]
        elif week[3] and first_thu and second_thu and not third_thu:
            third_thu = week[3]
        elif (week[3] and first_thu and second_thu and third_thu and
              not fourth_thu):
            fourth_thu = week[3]
        # Fridays
        if week[4] and not first_fri:
            first_fri = week[4]
        elif week[4] and first_fri and not second_fri:
            second_fri = week[4]
        elif week[4] and first_fri and second_fri and not third_fri:
            third_fri = week[4]
        elif (week[4] and first_fri and second_fri and third_fri and
              not fourth_fri):
            fourth_fri = week[4]
        # Saturdays
        if week[5] and not first_sat:
            first_sat = week[5]
        elif week[5] and first_sat and not second_sat:
            second_sat = week[5]
        elif week[5] and first_sat and second_sat and not third_sat:
            third_sat = week[5]
        elif (week[5] and first_sat and second_sat and third_sat and
              not fourth_sat):
            fourth_sat = week[5]
        # Sundays
        if week[6] and not first_sun:
            first_sun = week[6]
        elif week[6] and first_sun and not second_sun:
            second_sun = week[6]
        elif week[6] and first_sun and second_sun and not third_sun:
            third_sun = week[6]
        elif (week[6] and first_sun and second_sun and third_sun and
              not fourth_sun):
            fourth_sun = week[6]

    # Last Weekdays In Month
    today = dt.now()
    for day in range(days_in_month, 0, -1):
        if today.replace(day=day).weekday() == 0 and not last_mon:
            last_mon = day
        if today.replace(day=day).weekday() == 1 and not last_tue:
            last_tue = day
        if today.replace(day=day).weekday() == 2 and not last_wed:
            last_wed = day
        if today.replace(day=day).weekday() == 3 and not last_thu:
            last_thu = day
        if today.replace(day=day).weekday() == 4 and not last_fri:
            last_fri = day
        if today.replace(day=day).weekday() == 5 and not last_sat:
            last_sat = day
        if today.replace(day=day).weekday() == 6 and not last_sun:
            last_sun = day

    month_dict = {
        '1': 1,
        '2': 2,
        '3': 3,
        '4': 4,
        '5': 5,
        '6': 6,
        '7': 7,
        '8': 8,
        '9': 9,
        '10': 10,
        '11': 11,
        '12': 12,
        '13': 13,
        '14': 14,
        '15': 15,
        '16': 16,
        '17': 17,
        '18': 18,
        '19': 19,
        '20': 20,
        '21': 21,
        '22': 22,
        '23': 23,
        '24': 24,
        '25': 25,
        '26': 26,
        '27': 27,
        '28': 28,
        'LAST': days_in_month,
        'FIRST_MONDAY': first_mon,
        'SECOND_MONDAY': second_mon,
        'THIRD_MONDAY': third_mon,
        'FOURTH_MONDAY': fourth_mon,
        'LAST_MONDAY': last_mon,
        'FIRST_TUESDAY': first_tue,
        'SECOND_TUESDAY': second_tue,
        'THIRD_TUESDAY': third_tue,
        'FOURTH_TUESDAY': fourth_tue,
        'LAST_TUESDAY': last_tue,
        'FIRST_WEDNESDAY': first_wed,
        'SECOND_WEDNESDAY': second_wed,
        'THIRD_WEDNESDAY': third_wed,
        'FOURTH_WEDNESDAY': fourth_wed,
        'LAST_WEDNESDAY': last_wed,
        'FIRST_THURSDAY': first_thu,
        'SECOND_THURSDAY': second_thu,
        'THIRD_THURSDAY': third_thu,
        'FOURTH_THURSDAY': fourth_thu,
        'LAST_THURSDAY': last_thu,
        'FIRST_FRIDAY': first_fri,
        'SECOND_FRIDAY': second_fri,
        'THIRD_FRIDAY': third_fri,
        'FOURTH_FRIDAY': fourth_fri,
        'LAST_FRIDAY': last_fri,
        'FIRST_SATURDAY': first_sat,
        'SECOND_SATURDAY': second_sat,
        'THIRD_SATURDAY': third_sat,
        'FOURTH_SATURDAY': fourth_sat,
        'LAST_SATURDAY': last_sat,
        'FIRST_SUNDAY': first_sun,
        'SECOND_SUNDAY': second_sun,
        'THIRD_SUNDAY': third_sun,
        'FOURTH_SUNDAY': fourth_sun,
        'LAST_SUNDAY': last_sun
    }

    dw = DataWarehouse('admin')
    sql = """
    SELECT *
    FROM rio_branham.t_auto_tasks tat 
    WHERE tat.operational = 'Operational'
          AND tat.start_datetime <= SYSDATE
          AND tat.auto_recurrence = 'Monthly'
    """
    dw.query_results(sql)
    for task in dw.results:
        current_task = Task(task, dw.column_names)
        days = re.sub('[ ]?', '', current_task.task_data['month_days'])
        times = re.sub('[ ]?', '', current_task.task_data['times'])
        months = re.sub('[ ]?', '', current_task.task_data['months'])
        start_month = current_task.task_data['start_datetime'].month
        for month in months.split(','):
            if month.isdigit():
                months_since = dt.now().month - start_month
                cadence = int(month)
                if months_since % cadence:
                    continue
            elif month_num_dict[month.upper()] != dt.now().month:
                continue
            for day in days.split(','):
                for run_time in times.split(','):
                    s.enterabs(
                        dt_to_sec(
                            dt.now().replace(
                                day=month_dict[day.upper()],
                                hour=int(re.sub('^(\d+):.*', '\\1', run_time)),
                                minute=int(re.sub('.*:(\d+)', '\\1', run_time)),
                                second=0
                            )
                        ),
                        current_task.task_data['priority'],
                        current_task.run_task
                    )


# %% Scheduler

s = sched.scheduler(dt_to_sec)
s.enterabs(dt_to_sec(next_hour()), 1, hourly)
s.enterabs(dt_to_sec(next_day()), 1, daily)
s.enterabs(dt_to_sec(next_week()), 1, weekly)
s.enterabs(dt_to_sec(next_month()), 1, monthly)

# %% Production

if __name__ == '__main__':
    s.run()
