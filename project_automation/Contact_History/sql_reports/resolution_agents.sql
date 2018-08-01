WITH T1 AS (
    SELECT
      TICH.AGENT,
      TICH.TEAM,
      CASE
      WHEN TICH.DURATION_MINUTES > 0
        THEN 1
      ELSE 0
      END                   DURATION_COUNT,
      PRIMARY_DISPOSITION,
      TICH.SKILL,
      TICH.DURATION_MINUTES,
      TRUNC(TIC.START_DATE) START_DATE

    FROM T_IC_CONTACT_HISTORY TICH
      LEFT JOIN SOLAR.T_IC_CDR TIC
        ON TICH.CONTACT_ID = TIC.CONTACT_ID
    WHERE
      TICH.SKILL IN (
        'Dialer - CS - Outbound'
        , 'Dialer - CS - Resolution Leads'
        , 'Dialer - CS - VAO Robo'
      )
      AND TEAM = 'Service Support - Jeff'
      AND START_DATE >= TRUNC(SYSDATE) - 30
)
SELECT
  AGENT,
  TEAM,
  TO_CHAR(START_DATE, 'FMDD-MON-YY') START_DATE,
  SUM(DURATION_COUNT)             CALLED,
  COUNT(DURATION_COUNT)           QUEUED,
  ROUND(AVG(DURATION_MINUTES), 2) AVERAGE_DURATION,
  SUM(CASE
      WHEN PRIMARY_DISPOSITION = 'Not Contacted - Supervisor Attention Needed'
        THEN 1
      ELSE 0
      END)                        "Supervisor Attention Needed",
  SUM(CASE
      WHEN PRIMARY_DISPOSITION = 'Positive- Customer Contact Complete'
        THEN 1
      ELSE 0
      END)                        "Customer Contact Complete",
  SUM(CASE
      WHEN PRIMARY_DISPOSITION = 'Positive- Customer Info Complete'
        THEN 1
      ELSE 0
      END)                        "Customer Info Complete",
  SUM(CASE
      WHEN PRIMARY_DISPOSITION = 'Resolved - Not Ready'
        THEN 1
      ELSE 0
      END)                        "Resolved - Not Ready",
  SUM(CASE
      WHEN PRIMARY_DISPOSITION = 'Retry - Preview Requeue'
        THEN 1
      ELSE 0
      END)                        "Retry - Preview Requeue",
  SUM(CASE
      WHEN PRIMARY_DISPOSITION = 'Retry- Customer Info Attempt'
        THEN 1
      ELSE 0
      END)                        "Retry- Customer Info Attempt"
FROM T1
GROUP BY AGENT, START_DATE, TEAM
ORDER BY AGENT, T1.START_DATE DESC