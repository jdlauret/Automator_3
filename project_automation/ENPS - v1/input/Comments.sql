WITH
    cases_raw AS
    (
        SELECT
            ca.case_id
          , ca.case_number
          , pr.project_name
          , pr.service_state
          , pr.ROC_NAME
        FROM
            sfrpt.t_dm_case ca
        LEFT OUTER JOIN sf.sf_service se
        ON
            (
                ca.service_id = se.id
            )
        INNER JOIN sfrpt.t_dm_project pr
        ON
            (
                pr.project_id = ca.project_id
             OR
                (
                    ca.project_id IS NULL
                AND pr.project_id = se.solar_project
                )
            )
        WHERE
            ca.created_date >= {1}
          AND ca.created_date < {2}
        AND pr.{0}
    )


  , case_comments_raw AS
    (
        SELECT
            id AS case_comment_id
          , parentid
          , createddate                                                                                 AS comment_created_date
          , REPLACE( REPLACE( REPLACE( commentbody, CHR( 9 ), ' ' ), CHR( 10 ), ' ' ), CHR( 13 ), ' ' ) AS comment_body
        FROM
            sf.sf_casecomment
        WHERE
            createddate >= {1}
        AND createddate < {2}
    )

SELECT
  'case' comment_type
  ,  ca.case_id
  , ca.case_number
  , ccr.case_comment_id
  , ca.project_name
  , ca.service_state
  , ca.ROC_NAME
  , ccr.comment_created_date
  , ccr.comment_body
FROM
    cases_raw ca
  , case_comments_raw ccr
WHERE
    ca.case_id = ccr.parentid
<limit>