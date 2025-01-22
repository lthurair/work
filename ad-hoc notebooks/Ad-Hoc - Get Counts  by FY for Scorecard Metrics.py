# Databricks notebook source
# MAGIC %md
# MAGIC ********************************************************
# MAGIC **Purpose:** Get counts by FY for the following Scorecard Metrics: business credit cards, business credit cards incremental, merchant, cms, new chequing accounts, TD Securities, BCP, wealth referrals, GIC, net authorized volume, MUR, commercial volume for FY20 to FY24
# MAGIC
# MAGIC **Date:** Oct 8, 2024
# MAGIC
# MAGIC ********************************************************

# COMMAND ----------

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
import time
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

# MAGIC %md #### Create bbir views

# COMMAND ----------

create_temp_table(bbirczJdbcUrl,"bbir","psa_eligible_sales",start_date=20191101, end_date=20240930)
create_temp_table(bbirczJdbcUrl,"bbir","hrm_employee_profile",start_date=20191101, end_date=20240930)


# COMMAND ----------

# MAGIC %md #### Identify metrics by product codes for each FY

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view sc as
# MAGIC select transaction_id, product_code, current_amount, recognition_date, pr_acf2_logon_id_credited_to, cast(pr_branch_credited_to as int) as branch_credited, transaction_code, 
# MAGIC case when recognition_date between '2019-11-01' and '2020-10-31' then 'FY20'
# MAGIC when recognition_date between '2020-11-01' and '2021-10-31' then 'FY21'
# MAGIC when recognition_date between '2021-11-01' and '2022-10-31' then 'FY22'
# MAGIC when recognition_date between '2022-11-01' and '2023-10-31' then 'FY23'
# MAGIC when recognition_date between '2023-11-01' and '2024-08-19' then 'FY24'
# MAGIC end as fiscal_year, 
# MAGIC case when transaction_code in ('A','V') then 1 
# MAGIC when transaction_code in ('D','M','N') then -1  else 0 end as elig_ind,
# MAGIC case when product_code in ('V940','V990','V992','V930','V950') then 1 else 0 end as BBC_count,
# MAGIC case when product_code in ('V520','V975','V997','V999','V935','V955') then 1 else 0 end as BBC_incr_count,
# MAGIC case when product_code in ('V910','V960','V970') then 1 else 0 end as merchant_count,
# MAGIC case when product_code in ('C010','C020','C030','C040') then 1 else 0 end as cms_count,
# MAGIC --sum(case when product_code in ('D909','L899','L973','L999') then current_amount else 0 end) as com_volume,
# MAGIC case when product_code in ('D915') then 1 else 0 end as chequing_acct_count,
# MAGIC case when product_code in ('X810','X815') then 1 else 0 end as tds_fx_count,
# MAGIC case when product_code in ('I050') then 1 else 0 end as bcp_count,
# MAGIC --sum(case when product_code in ('M800','M850') then current_amount else 0 end) as mur_volume,
# MAGIC case when product_code in ('E160','K160','Y160','Y260','Y400','Y410') then 1 else 0 end as wealth_ref_count,
# MAGIC case when product_code in ('T008','T009','T043','T920','T921','T930','T943','T944','T990') then 1 else 0 end as gic_count,
# MAGIC ifw_effective_date
# MAGIC --need gnb points and retail referrals and tdfef counts
# MAGIC from psa_eligible_sales
# MAGIC where product_code in ('V940','V990','V992','V930','V950','V520','V975','V997','V999','V935','V955','V910','V960','V970','C010','C020','C030','C040','D909','L899','L973','L999','D915','X810','X815','I050','M800','M850','E160','K160','Y160','Y260','Y400','Y410','S610','S612','S615','S617','S620','S625','S630','S635','S640','S645','L530','L550','L555','L570','L600','L605','L630','S660','S662','S663','S667','S668','S670','S675','S680','S690','S650','S021','S026','L290','T008','T009','T043','T920','T921','T930','T943','T944','T990')
# MAGIC and (recognition_date between '2019-11-01' and '2024-09-30')

# COMMAND ----------

# MAGIC %md #### Aggregate counts

# COMMAND ----------

# MAGIC %sql
# MAGIC --aggregate
# MAGIC create or replace temporary view all_mets as
# MAGIC select branch_credited, fiscal_year, 
# MAGIC sum(case when bbc_count=1 then elig_ind else 0 end) as bbc_count_agg,
# MAGIC sum(case when bbc_incr_count=1 then elig_ind else 0 end) as bbc_incr_count_agg,
# MAGIC sum(case when merchant_count=1 then elig_ind else 0 end) as merchant_count_agg,
# MAGIC sum(case when cms_count=1 then elig_ind else 0 end) as cms_count_agg,
# MAGIC sum(case when chequing_acct_count=1 then elig_ind else 0 end) as cheq_acct_count_agg,
# MAGIC sum(case when tds_fx_count=1 then elig_ind else 0 end) as tds_fx_count_agg,
# MAGIC sum(case when bcp_count=1 then elig_ind else 0 end) as bcp_count_agg,
# MAGIC sum(case when wealth_ref_count=1 then elig_ind else 0 end) as wealth_ref_count_agg,
# MAGIC sum(case when gic_count=1 then elig_ind else 0 end) as gic_count_agg,
# MAGIC sum(case when product_code in ('S610','S612','S615','S617','S620','S625','S630','S635','S640','S645','L530','L550','L555','L570','L600','L605','L630','S660','S662','S663','S667','S668','S670','S675','S680','S690','S650','S021','S026','L290') then current_amount*elig_ind else 0 end) as net_auth_volume,
# MAGIC sum(case when product_code in ('M800','M850') then current_amount*elig_ind else 0 end) as mur_volume,
# MAGIC sum(case when product_code in ('D909','L899','L973','L999') then current_amount*elig_ind else 0 end) as com_volume
# MAGIC from sc
# MAGIC where branch_credited is not null
# MAGIC group by branch_credited, fiscal_year
# MAGIC order by branch_credited, fiscal_year desc

# COMMAND ----------

# MAGIC %md #### Get retail referrals

# COMMAND ----------

sqlQuery = '''(select lead_event_id, cust_id, asignd_to_branch_no, asignd_to_logon_id, lead_source_cd,
creatn_dt, closed_dt, 
case when closed_dt between '2019-11-01' and '2020-10-31' then 'FY20'
when closed_dt between '2020-11-01' and '2021-10-31' then 'FY21'
when closed_dt between '2021-11-01' and '2022-10-31' then 'FY22'
when closed_dt between '2022-11-01' and '2023-10-31' then 'FY23'
when closed_dt between '2023-11-01' and '2024-08-19' then 'FY24'
end as fiscal_year, 
lifecy_cd, lifecy_mn, lifecy_ds, lifecy_reason_cd, lifecy_reason_mn, lifecy_reason_ds, 
initve_prodct_target_cd, initve_prodct_target_ds, trim(upper(origtr_logon_id)) as orig_acf2, origtr_line_of_busnes_cd, origtr_line_of_busnes_mn, origtr_line_of_busnes_ds, origtr_branch_no, rcmndd_line_of_busnes_cd, rcmndd_line_of_busnes_mn, rcmndd_line_of_busnes_ds, recvng_line_of_busnes_cd, recvng_line_of_busnes_mn, recvng_line_of_busnes_ds, to_dt, ifw_effective_date
from caedw.leads 
where to_dt='9999-12-31'
and lead_source_cd=7332
and lifecy_reason_cd=3145
and (closed_dt between '2019-11-01' and '2024-08-19')
and initve_prodct_target_cd in ('3100','3101','3102','3103','3104','3139','3140','3195','3212','3213','3218','3223','3267','3272','7907','8907','8908','9984','9985','15726','7999','3216')
)t''' 
df12 = spark.read.jdbc(url=caedwczJdbcUrl,table=sqlQuery,properties=connectionProperties)
df12.createOrReplaceTempView("lead")
df12.display(10)

# COMMAND ----------

# MAGIC %md #### Create leads_branch view

# COMMAND ----------

sqlQuery = '''(select *
from caedw.leads_branch)t''' 
df12 = spark.read.jdbc(url=caedwczJdbcUrl,table=sqlQuery,properties=connectionProperties)
df12.createOrReplaceTempView("lead_branch")
df12.display(10)

# COMMAND ----------

# MAGIC %md #### Join leads data with leads_branch to get correct originating and receiving LOBs

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view all_ret as
# MAGIC select distinct a.lead_event_id, a.cust_id, a.asignd_to_branch_no, a.asignd_to_logon_id, a.lead_source_cd,
# MAGIC a.creatn_dt, a.closed_dt, a.lifecy_cd, a.lifecy_mn, a.lifecy_ds, a.lifecy_reason_cd, a.lifecy_reason_mn, a.lifecy_reason_ds, 
# MAGIC a.initve_prodct_target_cd, a.orig_acf2, a.origtr_branch_no, a.rcmndd_line_of_busnes_cd, a.rcmndd_line_of_busnes_mn, 
# MAGIC a.to_dt, a.ifw_effective_date, fiscal_year, 
# MAGIC b.branch_line_of_busnes_ds as new_orig_lob, c.branch_line_of_busnes_ds as new_assigned_lob
# MAGIC from lead a
# MAGIC left join 
# MAGIC lead_branch b 
# MAGIC on a.origtr_branch_no=b.branch_no
# MAGIC left join 
# MAGIC lead_branch c 
# MAGIC on a.asignd_to_branch_no=c.branch_no
# MAGIC where
# MAGIC --where a.ifw_effective_date>='20231101000000'
# MAGIC b.branch_line_of_busnes_ds in ('Small Business Banking','Commercial Banking','Small Business Banking Cash Management')
# MAGIC order by lead_event_id

# COMMAND ----------

# MAGIC %md #### Aggregate referrals

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view all_ret1 as
# MAGIC select origtr_branch_no, fiscal_year, count(lead_event_id) as retail_ref_count
# MAGIC from all_ret
# MAGIC group by origtr_branch_no, fiscal_year
# MAGIC order by origtr_branch_no, fiscal_year

# COMMAND ----------

# MAGIC %md #### Push to Synapse

# COMMAND ----------

df=spark.sql("select * from all_mets")
publish_to_synapse("analytics","sbb_scorecard_adhoc1",df)

# COMMAND ----------

df1=spark.sql("select * from all_ret1")
publish_to_synapse("analytics","sbb_scorecard_adhoc2",df1)

# COMMAND ----------

# MAGIC %md #### NEW AD-HOC - GET COUNTS OF GIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view sc as
# MAGIC select transaction_id, product_code, current_amount, recognition_date, pr_acf2_logon_id_credited_to, cast(pr_branch_credited_to as int) as branch_credited, transaction_code, 
# MAGIC case when recognition_date between '2019-11-01' and '2020-10-31' then 'FY20'
# MAGIC when recognition_date between '2020-11-01' and '2021-10-31' then 'FY21'
# MAGIC when recognition_date between '2021-11-01' and '2022-10-31' then 'FY22'
# MAGIC when recognition_date between '2022-11-01' and '2023-10-31' then 'FY23'
# MAGIC when recognition_date between '2023-11-01' and '2024-09-30' then 'FY24'
# MAGIC end as fiscal_year, 
# MAGIC case when transaction_code in ('A','V') then 1 
# MAGIC when transaction_code in ('D','M','N') then -1  else 0 end as elig_ind,
# MAGIC case when product_code in ('T008','T009','T043','T920','T921','T930','T943','T944','T990') then 1 else 0 end as gic_count,
# MAGIC ifw_effective_date, record_date
# MAGIC --need gnb points and retail referrals and tdfef counts
# MAGIC from psa_eligible_sales
# MAGIC where product_code in ('T008','T009','T043','T920','T921','T930','T943','T944','T990')
# MAGIC and (recognition_date between '2019-11-01' and '2024-09-30')

# COMMAND ----------

# MAGIC %md #### Identify only AMSB transactions

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view gn6 as
# MAGIC select a.*, case when trim(b.ident_prty_idntftn_num) is not null then 1 else 0 end as match_ind
# MAGIC from sc a
# MAGIC left join
# MAGIC sc4 b
# MAGIC on trim(a.pr_acf2_logon_id_credited_to)=trim(b.ident_prty_idntftn_num)
# MAGIC and a.record_date=b.record_date
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gn6 limit 4

# COMMAND ----------

# MAGIC %md #### Get count of GICs

# COMMAND ----------

# MAGIC %sql
# MAGIC select fiscal_year, match_ind as amsb_ind, sum(elig_ind)
# MAGIC from gn6
# MAGIC where fiscal_year='FY24'
# MAGIC group by fiscal_year, match_ind