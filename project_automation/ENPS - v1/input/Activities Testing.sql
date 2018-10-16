SELECT
  'activity' comment_type
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

LEFT OUTER JOIN sf.sf_service se
ON
    tk.what_type = 'Service'
AND tk.what_id = se.id

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
 OR se.solar_project = pr.project_id
 OR ca.project_id = pr.project_id
 OR cad.project_id = pr.project_id
 OR wo.project_id = pr.project_id

WHERE
    tk.created_date >= '01-JAN-17'
AND tk.description IS NOT NULL
AND pr.service_state = 'CA'
AND ROWNUM <= 500