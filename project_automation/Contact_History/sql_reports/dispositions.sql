WITH T1 AS (
    SELECT
      NVL(TICH.PRIMARY_DISPOSITION, 'EMPTY') PRIMARY_DISPOSITION,
      TRUNC(TIC.START_DATE)                  START_DATE
    FROM JDLAURET.T_IC_CONTACT_HISTORY TICH
      LEFT JOIN
      SOLAR.T_IC_CDR TIC
        ON TICH.CONTACT_ID = TIC.CONTACT_ID
    WHERE
      TICH.SKILL IN (
        'Dialer - CS - Outbound'
        , 'Dialer - CS - Resolution Leads'
        , 'Dialer - CS - VAO Robo'
      )
      AND START_DATE >= TRUNC(SYSDATE) - 30
),
    T2 AS (
      SELECT
        NVL(PRIMARY_DISPOSITION, 'Total') PRIMARY_DISPOSITION,
        COUNT(PRIMARY_DISPOSITION)        COUNT,
        GROUPING_ID(PRIMARY_DISPOSITION)  GID
      FROM T1
      GROUP BY GROUPING SETS ((PRIMARY_DISPOSITION), ())
      ORDER BY DECODE(PRIMARY_DISPOSITION, 'Total', 1, 0), COUNT DESC
  )
SELECT
  PRIMARY_DISPOSITION,
  COUNT,
  CASE
  WHEN PRIMARY_DISPOSITION = 'Total'
    THEN NULL
  ELSE
    ROUND(100 * RATIO_TO_REPORT(COUNT)
    OVER (
      PARTITION BY GID ), 2) || '%'
  END PERCENT
FROM T2