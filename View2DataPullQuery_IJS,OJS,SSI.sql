-- SSI WEEK LEVEL IJS/OJS order count, incident count -- 

-- CODE STARTS HERE -- 

select
	 
	count( distinct order_external_id) as total_orders 
	,sum(cnt_inc_id) as total_incidents 
	,P90_IJS
	,P90_OJS
	,round(  sum(cnt_inc_id) / count( distinct order_external_id) * 100) as Incident_to_order
	,ssi_column
	,issue_type	
	,sub_issue_type	
	,sub_sub_issue_type
					
	
	from 
		(select 
			order_external_id
			,total_ijs_score
			,P90_IJS
			,cnt_inc_id
			,percentile(cast(total_ijs_score as bigint),0.9) OVER (PARTITION BY ( ssi_column)) as P90_OJS	
			,ssi_column
			,issue_type	
			,sub_issue_type	
			,sub_sub_issue_type
				
		from 
			(
			select 
				order_external_id
				,sum(incident_ijs_score) as total_ijs_score
				,P90_IJS
				,cnt_inc_id
				,ssi_column
				,issue_type	
				,sub_issue_type	
				,sub_sub_issue_type
					
			from
				(select  
					distinct 
					order_external_id
					,incident_id
					,incident_ijs_score 
					,percentile(cast(incident_ijs_score as bigint),0.9) OVER (PARTITION BY ( concat(issue_type, sub_issue_type, sub_sub_issue_type))) as P90_IJS		
					,weekofyear(to_date(incident_creation_date)) as incident_week
					,to_date(incident_creation_date) as incident_creation_date
					,issue_type	
					,sub_issue_type	
					,sub_sub_issue_type
					,concat(issue_type, sub_issue_type, sub_sub_issue_type) as ssi_column
					,count ( distinct incident_id) over ( partition by order_external_id) as cnt_inc_id
					
				from 
					bigfoot_external_neo.mp_cs__ims_incident_hive_fact
				where 
					weekofyear(to_date(incident_creation_date))  = 2
				and 
					status_name = 'Solved'
				and order_external_id is not null 
				)a
			group by 
			P90_IJS
			,order_external_id
			,cnt_inc_id
			,ssi_column
			,issue_type	
			,sub_issue_type	
			,sub_sub_issue_type
					
			) b
		)c
group by 
P90_IJS
,P90_OJS 
,ssi_column
,issue_type	
,sub_issue_type	
,sub_sub_issue_type
					