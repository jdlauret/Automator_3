WITH T1 as (SELECT SUM(MANUAL_TIME) MANUAL_TIME,
                   MANUAL_RECURRENCE,
                   DEPARTMENT,
                   OWNERX,
                   SUM(CASE
                         WHEN MANUAL_RECURRENCE = 'Hourly'
                           -- Hours multiplied by working hours and working days
                                 THEN ROUND(((MANUAL_TIME / 60) * 8) * 5, 2)
                         WHEN MANUAL_RECURRENCE = 'Daily'
                           -- Hours multiplied by working days
                                 THEN ROUND((MANUAL_TIME / 60) * 5, 2)
                         WHEN MANUAL_RECURRENCE = 'Weekly'
                           -- Hours
                                 THEN ROUND((MANUAL_TIME / 60), 2)
                         WHEN MANUAL_RECURRENCE = 'Bi-Weekly'
                           -- Hours divided by 2
                                 THEN ROUND((MANUAL_TIME / 60) / 2, 2)
                         WHEN MANUAL_RECURRENCE = 'Monthly'
                           -- Hours divided by 4
                                 THEN ROUND((MANUAL_TIME / 60) / 4, 2)
                           END)     "HOURS SAVED"
            FROM D_POST_INSTALL.T_AUTO_TASKS
            WHERE OPERATIONAL NOT IN ('Non-Operational', 'Cycle')
            AND DEPARTMENT IS NOT NULL
            GROUP BY MANUAL_RECURRENCE, OWNERX, DEPARTMENT)
SELECT OWNERX,
       CASE
         WHEN OWNERX IN ('Mack', 'JD', 'Gavin')
                 THEN 21
         WHEN OWNERX IN ('Matthew Keeler', 'Grace')
                 THEN 17
         WHEN OWNERX IN ('Landon', 'Steven')
                 THEN 26
         WHEN OWNERX IS NOT NULL
                 THEN 15
           END                       PAY,
       SUM("HOURS SAVED")            "HOURS SAVED/WEEK",
       SUM("HOURS SAVED" * PAY)      "COST SAVED/WEEK",
       SUM("HOURS SAVED" * 52)       "HOURS SAVED/YEAR",
       SUM("HOURS SAVED" * 52 * PAY) "SAVINGS/YEAR",
       DEPARTMENT

FROM T1
GROUP BY GROUPING SETS ((OWNERX, DEPARTMENT));