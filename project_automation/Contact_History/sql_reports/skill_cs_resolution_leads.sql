WITH T1 AS (
    SELECT
      CASE
      WHEN TICH.DURATION_MINUTES > 0
        THEN 1
      ELSE 0
      END                   DURATION_COUNT,
      TICH.SKILL,
      TICH.DURATION_MINUTES,
      TRUNC(TIC.START_DATE) START_DATE

    FROM T_IC_CONTACT_HISTORY TICH
      LEFT JOIN SOLAR.T_IC_CDR TIC
        ON TICH.CONTACT_ID = TIC.CONTACT_ID
    WHERE
      TICH.SKILL = 'Dialer - CS - Resolution Leads'
      AND START_DATE >= TRUNC(SYSDATE) - 30
    ORDER BY  START_DATE ASC
)
SELECT
  TO_CHAR(START_DATE, 'FMDD-MON-YY') START_DATE,
  SUM(DURATION_COUNT)                                                  CALLED,
  COUNT(DURATION_COUNT)                                                QUEUED,
  ROUND((SUM(DURATION_COUNT) / COUNT(DURATION_COUNT)) * 100, 2) || '%' PERCENT_CALLED
FROM T1
GROUP BY START_DATE
ORDER BY T1.START_DATE DESC