select Project_name,
Project_status,
Service_Number,
service_state,
UTILITY_COMPANY,
Record_type,
Fund_name,
Installation_Complete,
ABCAD_Complete,
Fin_holds,
Finbacklog,
Fin_to_Process,
FIN_Rec,
FUND_Rec,
nvl(round(sysdate-initial_date - sum(case when issalesworkday = 0 then 1 else 0 end),0),0) Aging,
Pt2_Submit,
Pt2_Pending_Approval,
nvl(round(sysdate-int_approval - sum(case when issalesworkday = 0 then 1 else 0 end),0),0) Approval_Aging,
Pt2_Follow_ups,
Pt2_Rejected,
Total_BCs,
B1,
B2,
B3,
B4,
B5,
B6,
B7,
B8 - B8_4 as B8,
B9,
B10,
B11,
B12,
B15,
B16,
B17

from(select  p.project_name,
p.SERVICE_NUMBER,
p.project_status,
p.service_state,
p.UTILITY_COMPANY,
FND.record_type,
p.FUND_NAME,
p.Installation_Complete,
css.Fin_holds,
nvl((COALESCE(css.fin_scheduled,null)+coalesce((case when coalesce(css.fin_status,null)=1 and vatt.Fin_uploaded is not null then 1 end),0)),0) Finbacklog,
css.Fin_Process as Fin_to_Process,
p.INTER_POST_INSTALL_SUBMITTED,
BC.B1,
BC.B2,
BC.B3,
BC.B4 ,
BC.B5 ,
BC.B6 ,
BC.B7 ,
BC.B8,
BC.B8_4,
BC.B9 ,
BC.B10 ,
BC.B11 ,
BC.B12 ,
BC.B15 ,
BC.B16 ,
BC.B17 ,
(BC.B1+	BC.B2+	BC.B3+	BC.B4+	BC.B5+	BC.B6+	BC.B7+	BC.B8+	BC.B9+	BC.B10+	BC.B11+	BC.B12+		BC.B15+	BC.B16+	BC.B17-	BC.B8_4) as Total_BCs,
case when FND.Funding is not null and p.fin_received_date is not null and (BC.B1+	BC.B2+	BC.B3+	BC.B4+	BC.B5+	BC.B6+	BC.B7+	BC.B8+	BC.B9+	BC.B10+	BC.B11+	BC.B12+		BC.B15+	BC.B16+	BC.B17-	BC.B8_4 + BC.LP5) = 0 and p.INTER_POST_INSTALL_SUBMITTED is null then 1 else 0 end Pt2_Submit,
--case when FND.record_type in ('Solar PPA','Solar Lease') then greatest(FND.Agingdate,SFM.MaxFNDd) else FND.Agingdate end as Aging,
case when FND.record_type in ('Solar PPA','Solar Lease')and p.fin_received_date is not null and p.INTER_POST_INSTALL_SUBMITTED is null then greatest(SFM.MaxFNDd,FND.Agingdate) else FND.Agingdate end initial_date,
case when sysdate - p.INTER_POST_INSTALL_SUBMITTED >=15 and (p.INTERCONNECTION_STATUS not in ('Rejected') or p.INTERCONNECTION_STATUS is null) and (BC.B1+	BC.B2+	BC.B3+	BC.B4+	BC.B5+	BC.B6+	BC.B7+	BC.B8+	BC.B9+	BC.B10+	BC.B11+	BC.B12+		BC.B15+	BC.B16+	BC.B17-	BC.B8_4 +  BC.LP5)=0 and p.PTO_AWARDED is null then 1 else 0 end Pt2_Follow_ups,
case when (BC.B1+	BC.B2+	BC.B3+	BC.B4+	BC.B5+	BC.B6+	BC.B7+	BC.B8+	BC.B9+	BC.B10+	BC.B11+	BC.B12+		BC.B15+	BC.B16+	BC.B17-	BC.B8_4+ BC.LP5) = 0 and p.INTER_POST_INSTALL_SUBMITTED is not null and (p.INTERCONNECTION_STATUS not in ('Rejected') or p.INTERCONNECTION_STATUS is null) then 1 else 0 end Pt2_Pending_Approval,
case when (BC.B1+	BC.B2+	BC.B3+	BC.B4+	BC.B5+	BC.B6+	BC.B7+	BC.B8+	BC.B9+	BC.B10+	BC.B11+	BC.B12+		BC.B15+	BC.B16+	BC.B17-	BC.B8_4 +  BC.LP5) = 0 and p.INTER_POST_INSTALL_SUBMITTED is not null and (p.INTERCONNECTION_STATUS not in ('Rejected') or p.INTERCONNECTION_STATUS is null) then p.INTER_POST_INSTALL_SUBMITTED end int_approval,
case when FND.Funding is not null and p.fin_received_date is not null and p.INTERCONNECTION_STATUS in ('Rejected') and p.INTER_POST_INSTALL_SUBMITTED is not null then 1 else 0 end Pt2_Rejected,
case when p.fin_received_date is not null then 1 else 0 end Fin_Rec,
case when FND.Funding is not null then 1 else 0 end FUND_Rec,
p.AS_BUILT_CAD_COMPLETED as ABCAD_Complete


from sfrpt.t_dm_project p

--Holds
--left join ( select distinct p.project_ID, sum(case when h.type = 'Failed Inspection' then 1 end) as Fin_Holds
--from sfrpt.t_dm_project p
--left join sfrpt.t_dm_holds h on p.project_id = h.project_id group by p.project_ID) hld on p.PROJECT_ID = hld.project_id 

--FIN Case Statuses
left join (select distinct p.project_id, 
sum(case when cs.RECORD_TYPE like ('Solar - Electrical Inspection')and cs.status in ('Fail - Pending Fix','Pending LIT Review') then 1 else 0 end) as Fin_holds,
sum(case when cs.RECORD_TYPE like ('Solar - Electrical Inspection')and cs.status in ('Scheduled') then 1  end) as Fin_Scheduled,
sum(case when cs.record_type in ('Solar - Inspection') and cs.status in ('New','Pending FIN','In Progress') then 1  end) as Fin_status,
sum(case when cs.record_type in ('Solar - Inspection') and cs.status in ('FIN Uploaded') then 1 else 0 end) as Fin_Process
from sfrpt.t_dm_project p
left join sfrpt.t_dm_case cs on p.project_id=cs.project_id group by p.project_id ) css on p.project_id=css.project_id 

--vAttachment Fin Uploaded
left join (select distinct p.project_id, sum(case when vat.document_type = 'FIN' then 1  end)/count(case when vat.document_type = 'FIN' then 1  end) as Fin_uploaded
from sfrpt.t_dm_project p
left join sf.sf_vattachment vat on p.project_id=vat.PROJECT_SOLAR group by p.project_id) vatt on p.project_id=vatt.PROJECT_id

--Pending Issues
left join (select distinct p.project_id,
sum(case when lower(t.subject) like '%b1.%' then 1 else 0 end) B1,
sum(case when lower(t.subject) like '%b2.%' then 1 else 0 end) B2,
sum(case when lower(t.subject) like '%b3.%' then 1 else 0 end) B3,
sum(case when lower(t.subject) like '%b4.%' then 1 else 0 end) B4,
sum(case when lower(t.subject) like '%b5.%' then 1 else 0 end) B5,
sum(case when lower(t.subject) like '%b6.%' then 1 else 0 end) B6,
sum(case when lower(t.subject) like '%b7.%' then 1 else 0 end) B7,
sum(case when lower(t.subject) like '%b8.%' then 1 else 0 end) B8,
sum(case when lower(t.subject) like '%b8.4%' then 1 else 0 end) B8_4,
sum(case when lower(t.subject) like '%b9.%' then 1 else 0 end) B9,
sum(case when lower(t.subject) like '%b10.%' then 1 else 0 end) B10,
sum(case when lower(t.subject) like '%b11.%' then 1 else 0 end) B11,
sum(case when lower(t.subject) like '%b12.%' then 1 else 0 end) B12,
sum(case when lower(t.subject) like '%b15.%' then 1 else 0 end) B15,
sum(case when lower(t.subject) like '%b16.%' then 1 else 0 end) B16,
sum(case when lower(t.subject) like '%b17.%' then 1 else 0 end) B17,
sum(case when lower(t.subject) like '%lp5.6%' then 1 else 0 end) LP5
from sfrpt.t_dm_project p
left join sfrpt.t_dm_task t on t.phase_id=p.phase_id and lower(t.status) not in ('completed') group by p.project_id) BC on p.project_id=BC.project_id

--Contract Information 
left join( select distinct p.project_id,c.RECORD_TYPE,
sum(case when c.record_type in ('Solar Loan') and c.WO_SUBMITTED is not null and lower(c.LOAN_STATUS) in 'installation confirmed' then 1
when c.record_type in ('Solar Cash') then 1
when c.record_type in ('Solar PPA','Solar Lease') and f.contribution_funding_date is not null then 1  end) Funding,
case when c.record_type in ('Solar Loan') and p.fin_received_date is not null and p.INTER_POST_INSTALL_SUBMITTED is null then p.fin_received_date
when c.record_type in ('Solar Cash') and p.fin_received_date is not null and p.INTER_POST_INSTALL_SUBMITTED is null then p.fin_received_date
when c.record_type in ('Solar PPA','Solar Lease') and f.contribution_funding_date is not null and p.fin_received_date is not null and p.INTER_POST_INSTALL_SUBMITTED is null then greatest(p.fin_received_date,f.contribution_funding_date) end Agingdate


from sfrpt.t_dm_project p
left join sfrpt.mv_dm_funding f on f.project_id = p.project_id
left join sfrpt.t_dm_contract c on c.contract_id = p.primary_contract_id group by p.project_id, c.RECORD_TYPE, case when c.record_type in ('Solar Loan') and p.fin_received_date is not null and p.INTER_POST_INSTALL_SUBMITTED is null then p.fin_received_date when c.record_type in ('Solar Cash') and p.fin_received_date is not null and p.INTER_POST_INSTALL_SUBMITTED is null then p.fin_received_date when c.record_type in ('Solar PPA','Solar Lease') and f.contribution_funding_date is not null and p.fin_received_date is not null and p.INTER_POST_INSTALL_SUBMITTED is null then greatest(p.fin_received_date,f.contribution_funding_date) end 

 
)FND on p.project_id=FND.project_id
left join(select p.project_id,max(sf.TRANCHE_A_CAPITAL_CONTRIBUTION) as MaxFNDd
from sfrpt.t_dm_project p
left join sf.sf_solar_funding sf on sf.project=p.project_id group by p.project_id) SFM on p.project_id=SFM.project_id
where 
p.installation_complete is not null
and p.in_service_date is null and p.project_status in ('In Progress','Idle')


)
left join sfrpt.mv_dates d on d.dt between trunc(initial_date) and trunc(sysdate) 
group by project_name, project_status, record_type, SERVICE_NUMBER, service_state, 
initial_date, UTILITY_COMPANY, Installation_Complete, Fund_name, ABCAD_Complete, 
Fin_holds, Finbacklog, Fin_to_Process, FIN_Rec, B1, 
FUND_Rec, B2, B3, B4, B5, 
B6, B7, B8 - B8_4, B8_4, B8_4, 
int_approval, nvl(round(sysdate-int_approval,0),0), round(sysdate-int_approval,0), sysdate-int_approval, int_approval, 
0, 0, Pt2_Pending_Approval, Total_BCs, Pt2_Rejected, 
B9, B10, B11, B12, B15, B16, B17, Pt2_Submit, 
Pt2_Follow_ups
