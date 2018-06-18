Automator 3.0 - Scheduler Interface Documentation
=================================================

## Table of Contents
1. Description of Usage
2. Examples

## 1. Description of Usage
This document will provide a reference for all users of the
***Automator 3.0***, specifically on how to scheudule your own recurring
tasks.

First we will discuss the different auto-recurrence options, then we
will indicate specifically how to make sure your task runs at the times
you want. In the next section we will show multiple examples of how to
set up your task to run at different times.

### Auto-Recurrence Options:
* Hourly
* Daily
* Weekly
* Monthly

##### Hourly:
If you select `Hourly` in the `Auto-Recurrence` field then your task
will run
every hour of every day. You may specify what minute(s) your task runs
every
hour.

##### Daily:
If you select `Daily` in the `Auto-Recurrence` field then your task will
run
every day of the week. You may specifiy what time(s) of each day that
your
task runs.

##### Weekly:
If you select `Weekly` in the `Auto-Recurrence` field then your task
will run
every week. You may specifiy the cadence (every # weeks) what
day(s)
of the week and what time(s) of those days that you want your task to
run and.

##### Monthly:
If you select `Monthly` in the `Auto-Recurrence` field then your task
will run every month. You may specifiy the cadence (every # months) or
which months, what day(s) of the month as well as what time(s) of those
days that you want your task to run.

### How to Schedule a Task:

***Note**: Since we are using blank text fields you need to read this
section
carefully to make sure your tasks are entered correctly and will run.
Case
does not matter (Monday and MONDAY are the same) but spelling does.*

1. Select the `Start Datetime` for your task. Your task will
not run until this time allowing you to schedule a task that you don't
want to
start running until a specific time in the future.

2.  Select either `Hourly`, `Daily`, `Weekly` or `Monthly` from
the `Auto-Recurrence` drop-down.

3. For `Hourly` tasks you need to fill out the `Minutes` field. The
valid minute entries are the numbers `0-59`. If you want your task to
run at
multiple times each hour then separate the minutes by a comma
(`0,15,30,45`).

4. For `Daily`, `Weekly` and `Monthly` tasks you need to fill out the
`Times`
field. Valid time entries for this field are of the following format
`HH:MM`
and must be in 24-hour/military time. You may also specifiy multiple
times by
comma separation (`9:00,16:30`). This is the only other field you need
to fill
out for `Daily` tasks.

5. For `Weekly` tasks, in addition to the `Times` field you must also
fill out the `Week Days` field. Valid weekday entries are `Monday`,
`Tuesday`, `Wednesday`, `Thursday`, `Friday`, `Saturday` and `Sunday`.
Please fully spell the weekday name and make sure it is spelled
correctly. To specify multiple weekdays use comma separation
(`Monday,Friday`). To specify the cadence you must also fill out the
`Weeks` field. Valid `Weeks` entries are any number of weeks as long as
it is a whole number.

6. For `Monthly` tasks, in addition to the `Times` field you must also
fill out the `Month Days` field. There are many valid
`Month Day` options. The most basic are the days of the month `1-28`.
You may also specify the last day of the month with `Last`. Other
options are of the forms `ORDINAL_WEEKDAY` or `LAST_WEEKDAY`. Comma
separation is again used for multiple specifications
(`1,15,LAST, FIRST_MONDAY, THIRD_FRIDAY, LAST_SATURDAY`). You mas also
provide a comma separated list for the cadence. Either a whole number
specifying the number of months between runs or a list of the names
of the months you would like to have the report run
(`January,February`).

## 2. Examples
#### Hourly
To schedule a task every hour on the hour and on the 30 minute mark:
> Auto-Recurrence: `Hourly`   
> Minutes: `0,30`

#### Daily
To schedule a task every day at midnight and 6 A.M.:
> Auto-Recurrence: `Daily`  
> Times: `00:00,6:00`

#### Weekly
To schedule a task every week on Monday, Wednesday and Friday at 12:30
A.M. and 4:00 P.M.:
> Auto-Recurrence: `Weekly`  
> Week Days: `Monday,Wednesday,Friday`
> Times: `00:30,16:00`

To schedule a task every 3 weeks on Monday at 2:00 A.M.:
> Auto-Recurrence: `Weekly`
> Week Days: `Monday`
> Times: `2:00`
> Weeks: 3

#### Monthly
To schedule a task every month on the first Monday, 15th of the month
and last day of the month at 11:00 P.M.:
> Auto-Recurrence: `Monthly`  
> Month Days: `FIRST_MONDAY,15,Last`  
> Times: `23:00`

To schedule a task 3 months on the first of the month at 3:00 A.M:
> Auto-Recurrence: `Monthly`
> Month Days: `1`
> Times: `3:00`
> Months: `3`

To Schedule a task every January, June and December on the last day of
the month at 5:30 P.M.:
> Auto-Recurrence: `Monthly`
> Month Days: `LAST`
> Times: `17:30`
> Months: `January, June, December`

##### *Questions/Comments/Suggestions*
*Contact Rio @ rio.branham@vivintsolar.com*
