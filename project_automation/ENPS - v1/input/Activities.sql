SELECT
  'activity' comment_type
  , pr.ROC_NAME
  , pr.SERVICE_STATE
  , tk.task_id
  , tk.subject
  , pr.project_id
  , pr.project_name
  , tk.what_id
  , tk.what_type
  , tk.created_by
  , tk.created_date
  , tk.status
  , tk.description
  , DECODE( tk.status, 'Completed', 1, 0 ) AS is_completed

FROM
    sfrpt.t_dm_task tk

LEFT OUTER JOIN sfrpt.t_dm_service se
ON
    tk.what_type = 'Service'
AND tk.what_id = se.SERVICE_ID

LEFT OUTER JOIN sfrpt.t_dm_case ca
ON
    tk.what_type = 'Case'
AND tk.what_id = ca.case_id

LEFT OUTER JOIN sfrpt.t_dm_system_design cad
ON
    tk.what_type = 'CAD'
AND tk.what_id = cad.cad_id

LEFT OUTER JOIN sfrpt.t_dm_workorder wo
ON
    tk.what_type = 'WorkOrder'
AND tk.what_id = wo.workorder_id

INNER JOIN sfrpt.mv_cdm_projects pr
ON
    (
        tk.what_type = 'Phase'
    AND tk.what_id = pr.phase_id
    )
 OR se.PROJECT_ID = pr.project_id
 OR ca.project_id = pr.project_id
 OR cad.project_id = pr.project_id
 OR wo.project_id = pr.project_id

WHERE
    tk.created_date >= {1}
AND tk.created_date < {2}
AND tk.description IS NOT NULL
AND pr.{0}
<limit>

