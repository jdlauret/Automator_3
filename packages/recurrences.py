from . import *
import inspect

weekdays = [
        'Sunday',
        'Monday',
        'Tuesday',
        'Wednesday',
        'Thursday',
        'Friday',
        'Saturday',
    ]


def diff_month(d1, d2):
    return math.floor((d1.year - d2.year) * 12 + d1.month - d2.month)


def week_range(date):
    """Find all of the dates of the week for the given day.
    Assuming weeks start on Sunday and end on Saturday.

    Returns a list of all dates from requested week``.

    """
    # isocalendar calculates the year, week of the year, and day of the week.
    # dow is Mon = 1, Sat = 6, Sun = 7
    year, week, dow = date.isocalendar()

    # Find the first day of the week.
    if dow == 7:
        # Since we want to start with Sunday, let's test for that condition.
        start_date = date

    else:
        # Otherwise, subtract `dow` number days to get the first day
        start_date = date - dt.timedelta(dow)

    dates = list()
    # Now, add 6 for the last day of the week (i.e., count up to Saturday)
    dates.append(start_date.date())

    for i in range(6):
        next_date = start_date + dt.timedelta(days=i + 1)
        dates.append(next_date.date())

    return dates


def weekly_recurrence_date(recurrence_day):
    """
    This function is used to determine when the date the task should recur on should be
    :param recurrence_day: str: the recurrence day
    :return: datetime: the date that the task should recur on
    """
    today = dt.datetime.now()
    idx = (today.weekday() + 1) % 7
    last_sunday = today - dt.timedelta(7 + idx)
    last_week = week_range(last_sunday)
    this_week = week_range(today)

    last_week_recurrence_date = last_week[weekdays.index(recurrence_day.title())]
    this_week_recurrence_date = this_week[weekdays.index(recurrence_day.title())]

    if this_week_recurrence_date > today.date():
        return last_week_recurrence_date

    else:
        return this_week_recurrence_date


def recur_test(task_object):
    now = dt.datetime.now()
    # Get Last Run Date Time
    last_run_dt = task_object.last_run
    # If Last Run Datetime is not present it represents the first time
    # the task has been run and should simply just run

    if last_run_dt is None:
        return True

    # Get recurrence rate
    recurrence_rate = task_object.auto_recurrence

    # Get recurrence hour
    # If recurrence hour is None set to 0
    recurrence_hour = task_object.recurrence_hour
    if recurrence_hour is None:
        recurrence_hour = 0  # Default Midnight

    current_hour = now.hour

    # Get Recurrence Day
    # If Recurrence Day is None set to Monday
    recurrence_day = task_object.auto_recurrence_day
    if recurrence_day is None:
        recurrence_day = 'Monday'  # Default Monday

    try:
        recurrence_day_of_month = task_object.recurrence_day_of_month
    except:
        recurrence_day_of_month = None
    if not recurrence_day_of_month:
        recurrence_day_of_month = 1  # Default first of month

    # Get Date for recurrence day of the week


    current_day = now - dt.timedelta(minutes=now.minute) \
                  - dt.timedelta(seconds=now.second) \
                  - dt.timedelta(microseconds=now.microsecond)
    last_run_day = last_run_dt - dt.timedelta(minutes=last_run_dt.minute) \
                   - dt.timedelta(seconds=last_run_dt.second) \
                   - dt.timedelta(microseconds=last_run_dt.microsecond)

    hour_check = recurrence_hour <= now.hour
    day_check = (now.date() - last_run_dt.date()).days

    if recurrence_rate.lower() == 'hourly':
        if last_run_day < current_day:
            return True

    elif recurrence_rate.lower() == 'daily':
        if (current_day.date() - last_run_day.date()).days >= 1 \
                and recurrence_hour <= now.hour:
            return True

    elif recurrence_rate.lower() == 'weekly':
        recurrence_date = dt.datetime.combine(weekly_recurrence_date(recurrence_day), dt.time(0))
        if last_run_dt < recurrence_date \
                and recurrence_hour <= now.hour:
            return True

    elif recurrence_rate.lower() == 'monthly':

        if diff_month(now, last_run_dt) >= 1 \
                and now.day <= recurrence_day_of_month:
            return True

    return False


def recur_test_v2(last_run, rate, hour=0, day='Monday', day_of_month=1):

    if last_run is None:
        return True

    now = dt.datetime.now()

    recurrence_rate = rate
    recurrence_hour = hour
    recurrence_day = day
    recurrence_day_of_month = day_of_month

    if recurrence_rate.lower() == 'hourly':
        now = now.replace(microsecond=0, second=0, minute=0)
        last_run_at = last_run.replace(microsecond=0, second=0, minute=0)
        if last_run_at < now:
            return True

    elif recurrence_rate.lower() == 'daily':
        days_passed = (now.date() - last_run.date()).days

        if days_passed >= 1 \
                and recurrence_hour <= now.hour:
            return True

    elif recurrence_rate.lower() == 'weekly':
        # Get Date for recurrence day of the week
        recurrence_date = dt.datetime.combine(weekly_recurrence_date(recurrence_day), dt.time(0))

        if last_run < recurrence_date \
                and recurrence_hour <= now.hour:
            return True

    elif recurrence_rate.lower() == 'monthly':
        if diff_month(now, last_run) >= 1 \
                and now.day <= recurrence_day_of_month:
            return True

    return False
