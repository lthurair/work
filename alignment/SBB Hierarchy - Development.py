# Databricks notebook source
# MAGIC %md ### Set up connections

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run /Shared/Self_Migration_Jobs/Functions/Initialize_Connections

# COMMAND ----------

# MAGIC %run /Shared/Self_Migration_Jobs/Functions/Initialize_Functions

# COMMAND ----------

spark
import pandas as pd
import os
import numpy as np
#import time
import glob
from numpy import nan
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.functions import udf 
from pyspark.sql.functions import format_string
from pyspark.sql.types import DoubleType, IntegerType, StringType

pd.set_option('display.width',150)
pd.set_option('display.max_columns',None)
pd.set_option('display.max_rows',None)

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct record_date from dga_final.sbb_amsb_branch
# MAGIC order by record_date

# COMMAND ----------

# MAGIC %md ### get date from 2 months ago

# COMMAND ----------

end_date = get_first_last_dates_of_month(time_delta=-2,bus_ind=True,format="yyyy-mm-dd",return_value="end_date")
print(end_date)

# COMMAND ----------

# MAGIC %md ### Get max partition dates and create temp bbir tables

# COMMAND ----------

sqlQuery = f'''(
select max(hr.ifw_effective_date) as max_pod_date, max(hr.record_date) as max_record_date
from bbir.hrm_employee_profile hr
inner join bbir.bbdrc_calendar_umd cal
on hr.ifw_effective_date = cast(cal.full_dt as date)
and cal.busday_ind = 'Y'
inner join
bbir.bif_branch_information a
on hr.ifw_effective_date = a.ifw_effective_date
inner join
bbir.cif_rel_manager_profile b 
on hr.ifw_effective_date=b.ifw_effective_date
where hr.ifw_effective_date>='{end_date}'
and hr.job_clasfcn_cd = 'W01476'
--and hr.assoc_sts_type_cd = 'ACTIVE'
and (hr.job_profile_start_date is not null)
and a.branch_status like '%OPEN%'
and a.reporting_no_type like '%BRANCH%'
)'''

max_pod_date = run_sql(bbirczJdbcUrl,sqlQuery).collect()[0][0]
max_record_date = run_sql(bbirczJdbcUrl,sqlQuery).collect()[0][1]

print(max_pod_date, max_record_date)

# COMMAND ----------

create_temp_table(bbirczJdbcUrl,"bbir","hrm_employee_profile",end_date=max_record_date)
create_temp_table(bbirczJdbcUrl,"bbir","bif_branch_information",end_date=max_record_date)

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view hrm_temp as
# MAGIC select trim(upper(ident_prty_idntftn_num)) as logon,
# MAGIC trim(split(prefd_indiv_full_name,',')[1]) as fname, 
# MAGIC trim(prmry_fmly_name) as last_name,
# MAGIC prefd_indiv_full_name,
# MAGIC job_clasfcn_cd, 
# MAGIC job_clasfcn_desc as job_description, 
# MAGIC job_profile_start_date, 
# MAGIC calculated_fte, 
# MAGIC emplmt_bss_type_cd,
# MAGIC cc_intrnl_org_prty_id as cost_center, 
# MAGIC city_name,
# MAGIC assoc_sts_type_cd as empl_status, 
# MAGIC elctrnc_addr_txt as email___work, 
# MAGIC record_date
# MAGIC from hrm_employee_profile
# MAGIC where assoc_sts_type_cd = 'ACTIVE' and
# MAGIC (job_profile_start_date is not null)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view hrm_temp1 as
# MAGIC select case charindex(' ',fname,1) when 0 then fname 
# MAGIC else substring(fname,1,charindex(' ',fname,1)-1) end as first_name, *
# MAGIC from hrm_temp

# COMMAND ----------

# MAGIC %md ### Get AM and SM names

# COMMAND ----------

## Get AM Name
spark.sql("""
    create or replace temporary view am_name as
    select logon, prefd_indiv_full_name, first_name, last_name, job_clasfcn_cd, job_description, job_profile_start_date, calculated_fte, emplmt_bss_type_cd, am_cost_center, empl_status, area_manager_ind, email___work, record_date
    --mf, 
    --regexp_substr(ltrim(mf),'[A-z]*') as mname
    from
                    (select logon, 
                    first_name, 
                    last_name,
                    prefd_indiv_full_name,
                    fname,
                    last_name,
                    --substr(prefd_indiv_full_name,instr(prefd_indiv_full_name,',')+1) as mf,
                    job_clasfcn_cd, 
                    job_description, 
                    job_profile_start_date, 
                    calculated_fte, 
                    emplmt_bss_type_cd,
                    cost_center as am_cost_center, 
                    empl_status, 
                    1 as area_manager_ind,
                    email___work, 
                    record_date,
                    row_number() over(partition by cost_center order by logon) as cnt
                    from hrm_temp1 hr
                    where job_clasfcn_cd = 'W01476'
                    )f where cnt=1
""")


# COMMAND ----------

## Get SM Name
spark.sql("""
    create or replace temporary view sm_name as
    select first_name, last_name,
    --mf, regexp_substr(ltrim(mf),'[A-z]*') as sname,
    sm_cost_center, logon, empl_status, record_date
    from
    (
        select first_name, last_name, prefd_indiv_full_name,
        --substr(prefd_indiv_full_name,instr(prefd_indiv_full_name,',')+1) as mf,
        cost_center as sm_cost_center, logon, empl_status, record_date,
        row_number() over(partition by cost_center order by logon) as cnt
        from hrm_temp1 hr
        where job_clasfcn_cd = 'W01523'
    )f where cnt=1
""")

# COMMAND ----------

## Get AVP Name
spark.sql("""
    create or replace temporary view avp_name as
    select first_name, last_name,
    --mf, regexp_substr(ltrim(mf),'[A-z]*') as sname,
    sm_cost_center, logon, empl_status, record_date
    from
    (
        select first_name, last_name, prefd_indiv_full_name,
        --substr(prefd_indiv_full_name,instr(prefd_indiv_full_name,',')+1) as mf,
        cost_center as sm_cost_center, logon, empl_status, record_date,
        row_number() over(partition by cost_center order by logon) as cnt
        from hrm_temp1 hr
        where job_clasfcn_cd = 'W01695'
    )f where cnt=1
""")

# COMMAND ----------

# MAGIC %md ### Get max record_enddate from bbdrc_branch_hierarchy -- 99991231

# COMMAND ----------

sqlQuery = '''(
select max(record_enddate) as max_record_enddate
from bbir.bbdrc_branch_hierarchy
where branch_attribute_name ='SBB_DISTRICT_ID'
)'''

max_record_enddate = run_sql(bbirczJdbcUrl,sqlQuery).collect()[0][0]
print(max_record_enddate)

# COMMAND ----------

# MAGIC %md ### (1) SBB Alignment

# COMMAND ----------

import datetime
ct=datetime.datetime.now()
print(ct)

# COMMAND ----------

sqlQuery = '''(select *
from bbir.bbdrc_branch_hierarchy)t''' 
df4 = spark.read.jdbc(url=bbirczJdbcUrl,table=sqlQuery,properties=connectionProperties)
df4.createOrReplaceTempView("bbdrc_branch_hierarchy")


# COMMAND ----------

cte_sql_query=f'''
with br as
(
select distinct a.reporting_no as branch, branch_title as branch_name, branch_status, branch_attribute_value as am_cost_center, branch_province as province, record_date
from
bbir.bif_branch_information a
inner join
bbir.bbdrc_branch_hierarchy b 
on a.reporting_no=b.branch
where a.record_date = {max_record_date}
and a.branch_status like '%OPEN%'
and a.reporting_no_type like '%BRANCH%'
and b.record_enddate = {max_record_enddate}
and b.branch_attribute_name ='SBB_DISTRICT_ID'
),
am_cc_name as
(
select distinct branch, branch_attribute_value as am_cost_center_name
from bbir.bbdrc_branch_hierarchy
where record_enddate = {max_record_enddate}
and branch_attribute_name = 'SBB_DISTRICT_NAME_EN'
),
sm_cc as
(
select distinct branch, branch_attribute_value as sm_cost_center
from bbir.bbdrc_branch_hierarchy
where record_enddate = {max_record_enddate}
and branch_attribute_name = 'SBB_REGION_ID'
),
sm_cc_name as 
(
select distinct branch, branch_attribute_value as sm_cost_center_name
from bbir.bbdrc_branch_hierarchy
where record_enddate = {max_record_enddate}
and branch_attribute_name = 'SBB_REGION_NAME_EN'
),
retail_dis as
(
select distinct branch, branch_attribute_value as retail_district_id_b
from bbir.bbdrc_branch_hierarchy
where record_enddate = {max_record_enddate}
and branch_attribute_name = 'RETAIL_DISTRICT_ID'
),
retail_dis_en as
(
select distinct branch, branch_attribute_value as retail_district_name_en
from bbir.bbdrc_branch_hierarchy
where record_enddate = {max_record_enddate}
and branch_attribute_name = 'RETAIL_DISTRICT_NAME_EN'
),
retail_reg_id as
(
select distinct branch, branch_attribute_value as retail_region_id_b
from bbir.bbdrc_branch_hierarchy
where record_enddate = {max_record_enddate}
and branch_attribute_name = 'RETAIL_REGION_ID'
),
retail_reg_name as
(
select distinct branch, branch_attribute_value as retail_region_name_en
from bbir.bbdrc_branch_hierarchy
where record_enddate = {max_record_enddate}
and branch_attribute_name = 'RETAIL_REGION_NAME_EN'
),
ramsb_all as
(
select distinct branch, branch_attribute_value as alt_am_cost_ctr
from bbir.bbdrc_branch_hierarchy
where record_enddate = {max_record_enddate}
and branch_attribute_name = 'SBB_ALTERNATE_DISTRICT_ID'
),
final as 
(
select a.branch, a.branch_name, a.branch_status, a.am_cost_center, am_cost_center_name,
sm_cost_center, sm_cost_center_name, alt_am_cost_ctr, retail_district_id_b, retail_district_name_en, retail_region_id_b, retail_region_name_en, province, '{ct}' AS last_insert_timestamp, a.record_date
from 
br a
left join
am_cc_name c
on a.branch=c.branch
left join
sm_cc d 
on a.branch=d.branch
left join
sm_cc_name e 
on a.branch=e.branch
left join
retail_dis f 
on a.branch=f.branch
left join
retail_dis_en g
on a.branch=g.branch
left join
retail_reg_id h 
on a.branch=h.branch
left join
retail_reg_name i 
on a.branch=i.branch
left join
ramsb_all j 
on a.branch=j.branch
where a.am_cost_center is not null
)
'''

sql_query='select * from final'

df=run_sql(bbirczJdbcUrl,sql_query,cte=cte_sql_query)
df.createOrReplaceTempView(name="SBB_ALIGNMENT_STAGE")
display(df.sort(['branch']))


# COMMAND ----------

# MAGIC %md ### Merge the AM, SM, and AVP names

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view SBB_ALIGNMENT as
# MAGIC select a.branch, 
# MAGIC a.branch_name, 
# MAGIC a.branch_status, 
# MAGIC a.am_cost_center, 
# MAGIC a.am_cost_center_name, 
# MAGIC trim(COALESCE(concat(k.first_name,' ',k.last_name),'TBD')) as area_manager, 
# MAGIC k.logon as am_logon,
# MAGIC a.sm_cost_center, 
# MAGIC a.sm_cost_center_name, 
# MAGIC trim(coalesce((concat(l.first_name,' ',l.last_name)), (concat(p.first_name,' ',p.last_name)), 'TBD')) as senior_manager, 
# MAGIC coalesce(l.logon, p.logon) as sm_logon, 
# MAGIC a.alt_am_cost_ctr, 
# MAGIC a.retail_district_id_b, 
# MAGIC a.retail_district_name_en, 
# MAGIC a.retail_region_id_b, 
# MAGIC a.retail_region_name_en, 
# MAGIC a.province, 
# MAGIC a.last_insert_timestamp, a.record_date
# MAGIC from
# MAGIC SBB_ALIGNMENT_STAGE a
# MAGIC left join
# MAGIC am_name k
# MAGIC on a.am_cost_center=k.am_cost_center
# MAGIC left join
# MAGIC sm_name l
# MAGIC on a.sm_cost_center=l.sm_cost_center
# MAGIC left join
# MAGIC avp_name p
# MAGIC on a.sm_cost_center=p.sm_cost_center

# COMMAND ----------

# MAGIC %md ### Data quality check to make sure branch is not double counted

# COMMAND ----------

# MAGIC %sql
# MAGIC select branch, count(*) as cnt 
# MAGIC from SBB_ALIGNMENT
# MAGIC group by branch
# MAGIC having count(*)>1

# COMMAND ----------

# MAGIC %md ### Create a permanent table for Azure

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO table dga_final.sbb_alignment_out
# MAGIC select branch as branch_id, 
# MAGIC branch_name, 
# MAGIC am_cost_center, 
# MAGIC am_cost_center_name, 
# MAGIC am_logon, 
# MAGIC area_manager, 
# MAGIC sm_cost_center, 
# MAGIC sm_cost_center_name, 
# MAGIC sm_logon, 
# MAGIC senior_manager, 
# MAGIC province, 
# MAGIC alt_am_cost_ctr, 
# MAGIC case when alt_am_cost_ctr is not null then 'REMOTE RAMSB' else '' end as alt_am_cc_name, 
# MAGIC retail_region_id_b as retail_region_id, 
# MAGIC retail_region_name_en as retail_region_name, 
# MAGIC retail_district_id_b as retail_district_id, 
# MAGIC retail_district_name_en as retail_district_name, 
# MAGIC record_date
# MAGIC from SBB_ALIGNMENT
# MAGIC

# COMMAND ----------

# MAGIC %md ### PUSH OUTPUT 1 TO SYNAPSE - SBB ALIGNMENT EXCEL FOR SHARED DRIVE

# COMMAND ----------

df=spark.sql("select * from dga_final.sbb_alignment_out")
publish_to_synapse("analytics","sbb_alignment_out",df)

# COMMAND ----------

# MAGIC %md ### (2) Create the SBB Team Hierarchy (Buddy Branch)

# COMMAND ----------

# MAGIC %md #### get the AMSB names

# COMMAND ----------

spark.sql("""
    create or replace temporary view amsb_name as
    select logon, prefd_indiv_full_name, first_name, last_name,
    job_clasfcn_cd, job_description, job_profile_start_date, 
    calculated_fte, emplmt_bss_type_cd, am_cost_center, empl_status, amsb_ind, email___work, record_date
    from
                    (select prefd_indiv_full_name,first_name, last_name, email___work,
                    logon, job_clasfcn_cd, 1 as amsb_ind,
                    job_description, job_profile_start_date, calculated_fte, 
                    emplmt_bss_type_cd, cost_center as am_cost_center, empl_status, record_date,
                    row_number() over(partition by logon order by cost_center) as cnt
                    from hrm_temp1 hr
                    where job_clasfcn_cd='W01446'
                    )f where cnt=1
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from amsb_name

# COMMAND ----------

cte_sql_query=f'''
with br_hier as
(
SELECT branch,
	cast(substring(branch_attribute_value,1,5) as int) as cc_team,
	cast(substring(branch_attribute_value,7,3) as int) as team_no
FROM bbir.bbdrc_branch_hierarchy
where record_enddate = {max_record_enddate}
and branch_attribute_name = 'PORTFOLIO_ID'
group by branch, branch_attribute_value
),
cif_rel as
(
SELECT distinct cost_centre, teamno, TRIM(UPPER(logon_id)) AS logon, record_date
from bbir.cif_rel_manager_profile
where ifw_effective_date='{max_pod_date}'
),
final as 
(
select distinct a.branch, b.logon, b.cost_centre, b.teamno, 
'{ct}' AS last_insert_timestamp, b.record_date
from br_hier a 
left join 
cif_rel b
on a.cc_team=b.cost_centre
and a.team_no=b.teamno
)
'''

sql_query='select * from final'

df=run_sql(bbirczJdbcUrl,sql_query,cte=cte_sql_query)
df.createOrReplaceTempView(name="sbb_buddy_stage0")
display(df.sort(['branch']))


# COMMAND ----------

# MAGIC %md ### Create a permanent table for buddy branch alignment

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into table dga_final.sbb_amsb_branch
# MAGIC select distinct b.branch, 
# MAGIC b.teamno, 
# MAGIC coalesce(a.logon, c.logon) as acf2, 
# MAGIC coalesce(a.first_name,c.first_name) as first_name,
# MAGIC coalesce(a.last_name, c.last_name) as last_name, 
# MAGIC trim(COALESCE(concat((coalesce(a.first_name,c.first_name)),' ',(coalesce(a.last_name,c.last_name))),'TBD')) as name,
# MAGIC coalesce(a.job_clasfcn_cd,c.job_clasfcn_cd) as job_code, 
# MAGIC coalesce(a.job_description,c.job_description) as job_description,
# MAGIC coalesce(a.job_profile_start_date, c.job_profile_start_date) as job_profile_start_date, 
# MAGIC coalesce(a.emplmt_bss_type_cd, c.emplmt_bss_type_cd) as full_part_time, 
# MAGIC coalesce(a.calculated_fte, c.calculated_fte) as calculated_fte,
# MAGIC coalesce(a.empl_status, c.empl_status) as empl_status,
# MAGIC case when amsb_ind=1 then 'Y' else 'N' end as amsb_ind,
# MAGIC case when area_manager_ind=1 then 'Y' else 'N' end as area_manager_ind,
# MAGIC coalesce(a.email___work,c.email___work) as email___work,
# MAGIC current_timestamp() as last_insert_timestamp, b.record_date
# MAGIC from 
# MAGIC sbb_buddy_stage0 b
# MAGIC left join 
# MAGIC amsb_name a
# MAGIC on b.logon=a.logon
# MAGIC left join
# MAGIC am_name c
# MAGIC on b.logon=c.logon
# MAGIC where coalesce(a.logon, c.logon) is not null --removes inactive, departed people
# MAGIC --where branch is not null --do not remove null branches because RAMSB need to be included for other outputs
# MAGIC order by b.branch

# COMMAND ----------

# MAGIC %md #### check if there are duplicates

# COMMAND ----------

# MAGIC %sql
# MAGIC select branch, acf2, name, count(*) as cnt, record_date
# MAGIC from dga_final.sbb_amsb_branch
# MAGIC group by branch, acf2, name, record_date
# MAGIC having count(*)>1

# COMMAND ----------

# MAGIC %md ### PUSH OUTPUT2 TO SYNAPSE - BUDDY BRANCH

# COMMAND ----------

df=spark.sql("select * from dga_final.sbb_amsb_branch")
publish_to_synapse("analytics","sbb_amsb_branch",df)

# COMMAND ----------

# MAGIC %md ### Now, Run the BBIS Notebook