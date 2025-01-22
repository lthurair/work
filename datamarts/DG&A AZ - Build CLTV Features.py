# Databricks notebook source
# MAGIC %md ### Set up Connections

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
#import datetime
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

## edw tables - CZ
sql_cz_metadata = '''(SELECT * FROM INFORMATION_SCHEMA.TABLES) t '''
spark.read.jdbc(url=caedwczJdbcUrl,table=sql_cz_metadata,properties=connectionProperties).display()

## BBCRM and EXC tables - SRZ
sql_srz_metadata = '''(SELECT * FROM INFORMATION_SCHEMA.TABLES) t '''
spark.read.jdbc(url=srzJdbcUrl,table=sql_srz_metadata,properties=connectionProperties).display()

## BBIR views - bbir CZ
sql_bbir_metadata = '''(SELECT * FROM INFORMATION_SCHEMA.TABLES) t '''
spark.read.jdbc(url=bbirczJdbcUrl,table=sql_bbir_metadata,properties=connectionProperties).display()


# COMMAND ----------

# MAGIC %md ### create the views

# COMMAND ----------

create_temp_table(bbirczJdbcUrl,"bbir","das_business_account",start_date='2022-01-01',end_date='2024-03-31')
create_temp_table(bbirczJdbcUrl,"bbir","cif_customer_account_rel",start_date='2022-01-01',end_date='2024-03-31')

sqlQuery = '''(select *,'USD' as usd_flag
from bbir.bbdrc_bb_rates 
where record_enddate>='20220101' 
and product_key='USFX_MTH_AVG' and product_attribute_name='RATE_VALUE')t''' 
df = spark.read.jdbc(url=bbirczJdbcUrl,table=sqlQuery,properties=connectionProperties)

create_temp_table(bbirczJdbcUrl,"bbir","lon_loan_information",start_date='2022-01-01',end_date='2024-03-31')
create_temp_table(bbirczJdbcUrl,"bbir","psa_transaction",start_date='2021-10-01',end_date='2024-03-31')

sqlQuery = '''(select * from bbir_reports.bbir_caedw_token_bridge)t''' 
df0 = spark.read.jdbc(url=bbirczJdbcUrl,table=sqlQuery,properties=connectionProperties)
df0.createOrReplaceTempView("token_bridge")

sqlQuery = '''(select lead_event_id, cust_id, creatn_dt, closed_dt, lifecy_mn, lifecy_ds, lifecy_cd,
lifecy_reason_cd, lifecy_reason_ds, lead_am, lead_amount_curncy_mn, lead_source_ds, lead_source_mn, initve_prodct_target_ds, 
UPPER(trim(origtr_logon_id)) as orig_acf2, origtr_branch_no, 
upper(trim(asignd_to_logon_id)) AS receiving_acf2, asignd_to_branch_no AS receiving_branch_no,
last_change_dt, action_dt, to_dt,ifw_effective_date
from caedw.leads where to_dt='9999-12-31')t''' 
df1 = spark.read.jdbc(url=caedwczJdbcUrl,table=sqlQuery,properties=connectionProperties)
df1.createOrReplaceTempView("lead")

sqlQuery = '''(select cust_id, cust_type_mn, tdct_cust_in, tdam_cust_in, tdevg_cust_in, tdwh_cust_in,
tdfs_source_in, branch_of_rec_no, oprtnl_relatn_stat_ds, relatn_admin_mn, full_busnes_na1, moody_rating_cd, 
moody_rating_mn, moody_rating_ds, moody_rating_dt, major_risk_rating_ds, to_dt, ifw_effective_date
from caedw.cust where cust_type_mn='N' and to_dt>='2022-01-01')t''' 
df2 = spark.read.jdbc(url=caedwczJdbcUrl,table=sqlQuery,properties=connectionProperties)
df2.createOrReplaceTempView("cust")

sqlQuery = '''(select cif_business_customer_key_token, customr_num, customr_type, customr_branch_of_record, customr_name, customr_since_d, customr_status, customr_industry_c, primary_segment, ifw_effective_date, record_date
from bbir.cif_business_customer where ifw_effective_date in ('2022-01-31','2022-02-28','2022-03-31','2022-04-30','2022-05-31','2022-06-30','2022-07-31','2022-08-31','2022-09-30','2022-10-31','2022-11-30','2022-12-31','2023-01-31','2023-02-28','2023-03-31','2023-04-30','2023-05-31','2023-06-30','2023-07-31','2023-08-31','2023-09-30','2023-10-31','2023-11-30','2023-12-31','2024-01-31','2024-02-29', '2024-03-31'))t''' 
df3 = spark.read.jdbc(url=bbirczJdbcUrl,table=sqlQuery,properties=connectionProperties)
df3.createOrReplaceTempView("cif_business_customer")

sqlQuery = '''(select customr_industry_c, industry, segment, sub_segment, sic_title, professional_flg
from bbir.bbdrc_sic_ref)t''' 
df4 = spark.read.jdbc(url=bbirczJdbcUrl,table=sqlQuery,properties=connectionProperties)
df4.createOrReplaceTempView("sic_ref")

## mcrib data
mcrib = '''(select account_key, branch_num, account_num, tst, tst_description, reportperiod, currency, channel, nii,  record_date, ifw_effective_date
from bbir.euc_mcrib where ifw_effective_date between '2024-02-01' and '2024-02-29')t'''
df9 = spark.read.jdbc(url=bbirczJdbcUrl, table=mcrib, properties=connectionProperties)
df9.createOrReplaceTempView('mcrib')

# COMMAND ----------

# MAGIC %md ### create new views

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view cust1 as
# MAGIC select * from
# MAGIC (select cust_id, cust_type_mn, tdct_cust_in, tdam_cust_in, tdevg_cust_in, tdwh_cust_in,
# MAGIC tdfs_source_in, branch_of_rec_no, oprtnl_relatn_stat_ds, relatn_admin_mn, full_busnes_na1, moody_rating_cd, 
# MAGIC moody_rating_mn, moody_rating_ds, moody_rating_dt, major_risk_rating_ds, to_dt, ifw_effective_date,
# MAGIC row_number() over(partition by cust_id order by to_dt desc) as rn
# MAGIC from cust)k 
# MAGIC where rn=1 

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view tmp_token_bridge as
# MAGIC select *, substring(bbir_record_date,1,6) as rec2 from
# MAGIC     (select *,
# MAGIC     row_number() over(partition by bbir_key_token, substring(bbir_record_date,1,6) order by bbir_record_date desc) as rw
# MAGIC     from token_bridge where bbir_aplictn_id='CIF')h
# MAGIC where rw=1

# COMMAND ----------

# MAGIC %md ### 1) Create NII Metric

# COMMAND ----------

# MAGIC %md #### a) count records in mcrib

# COMMAND ----------

spark.sql("select count(*) as cnt from mcrib").display()

# COMMAND ----------

# MAGIC %md #### b) merge mcrib to cif_customer_account_rel view

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view rev as
# MAGIC select distinct b.customr_num, a.account_key, a.branch_num, a.account_num, tst, tst_description, reportperiod, currency, channel, nii, a.record_date
# MAGIC from mcrib a 
# MAGIC left join
# MAGIC cif_customer_account_rel b 
# MAGIC on a.account_key=b.account_key
# MAGIC and substring(a.record_date,1,6)=substring(b.record_date,1,6)
# MAGIC

# COMMAND ----------

# MAGIC %md #### c) count records to see if merge has same # of records

# COMMAND ----------

spark.sql("select count(*) as cnt from rev").display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select customr_num, account_key, record_date, count(*) as cnt  
# MAGIC from rev 
# MAGIC group by customr_num, account_key, record_date
# MAGIC having count(*)>1

# COMMAND ----------

# MAGIC %md #### d) group NII by CIF

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view rev3 as
# MAGIC select customr_num, substring(reportperiod,1,6) as year_month, sum(nvl(nii,0)) as total_nii 
# MAGIC from rev
# MAGIC group by customr_num, substring(reportperiod,1,6)

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table dga_sandbox.cltv_nii_staging as
# MAGIC select customr_num, year_month, total_nii 
# MAGIC from rev3

# COMMAND ----------

# MAGIC %md ### 2) create customer table

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table dga_sandbox.cltv_cust_staging as 
# MAGIC select distinct a.customr_num, a.cif_business_customer_key_token, c.caedw_id, 
# MAGIC a.customr_type, a.customr_branch_of_record, a.customr_name, a.customr_since_d, (a.customr_since_d/30) as cust_tenure_months, a.customr_status, a.customr_industry_c, a.primary_segment, a.ifw_effective_date, 
# MAGIC b.industry, b.segment, b.sub_segment, b.sic_title, b.professional_flg, 
# MAGIC v.cust_id, v.cust_type_mn, v.tdct_cust_in, v.tdam_cust_in, v.tdevg_cust_in, v.tdwh_cust_in, v.tdfs_source_in, v.branch_of_rec_no, 
# MAGIC --v.oprtnl_relatn_stat_ds, v.relatn_admin_mn, v.full_busnes_na1, 
# MAGIC v.moody_rating_cd, v.moody_rating_mn, v.moody_rating_ds, v.moody_rating_dt, v.major_risk_rating_ds, substring(a.record_date,1,6) as year_month
# MAGIC     from
# MAGIC     cif_business_customer a ------------contains last day of the month
# MAGIC     left join
# MAGIC     sic_ref b -----------one record
# MAGIC     on trim(a.customr_industry_c)=trim(b.customr_industry_c)
# MAGIC     left join
# MAGIC     tmp_token_bridge c
# MAGIC     on a.cif_business_customer_key_token=c.bbir_key_token
# MAGIC     and substring(a.record_date,1,6)=c.rec2
# MAGIC     left join
# MAGIC     cust1 v ---------cx records are not consistent in to_dt or ifw_effective_date
# MAGIC     on c.caedw_id=v.cust_id
# MAGIC     --where primary_segment='M$B'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dga_sandbox.cltv_cust_staging limit 10

# COMMAND ----------

# MAGIC %md #### data quality for customer table is good

# COMMAND ----------

# MAGIC %sql
# MAGIC select customr_num, year_month, count(*)
# MAGIC from dga_sandbox.cltv_cust_staging
# MAGIC group by customr_num, year_month
# MAGIC having count(*)>1

# COMMAND ----------

# MAGIC %md ### 3) create loan table

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table cltv.cltv_loan_staging as
# MAGIC --insert into table cltv.cltv_loan_staging
# MAGIC select customr_num, year_month,
# MAGIC 	sum(coalesce(auth_amt,0)) as vol_total_authamt_loans,
# MAGIC 	sum(coalesce(avg_bal,0)) as vol_total_avgbal_loans,
# MAGIC 	sum(coalesce(os_amt,0)) as vol_total_os_loans,
# MAGIC
# MAGIC 	sum(case when LON_STATUS_FULL='Active' then 1 else 0 end) as cnt_active_loans,
# MAGIC 	sum(case when LON_STATUS_FULL='Active' then proceed_amt else 0 end) as vol_active_proceed_loans,
# MAGIC 	sum(case when LON_STATUS_FULL='Active' then os_amt else 0 end) as vol_active_os_loans,
# MAGIC 	sum(case when LON_STATUS_FULL='Active' then paymnt_amt else 0 end) as vol_active_paymnt_loans,
# MAGIC 	sum(case when LON_STATUS_FULL='Active' then partal_paymnt_amt else 0 end) as vol_active_partial_paymnt_loans,
# MAGIC 	sum(case when LON_STATUS_FULL='Active' then avg_bal else 0 end) as vol_active_avgbal_loans,
# MAGIC 	sum(case when LON_STATUS_FULL='Active' then auth_amt else 0 end) as vol_active_authamt_loans,
# MAGIC 	avg(case when LON_STATUS_FULL='Active' then amortztn_months else 0 end) as cnt_active_amortztn_mos_loans,
# MAGIC 	count(distinct tst) as cnt_distinct_tst_loans,
# MAGIC 	
# MAGIC 	sum(case when LON_STATUS_FULL='Closed' then 1 else 0 end) as cnt_closed_loans,
# MAGIC 	sum(case when LON_STATUS_FULL='Closed' then proceed_amt else 0 end) as vol_proceedamt_closed_loans,
# MAGIC 	sum(case when LON_STATUS_FULL='Closed' then avg_bal else 0 end) as vol_avgbal_closed_loans,
# MAGIC 	
# MAGIC 	sum(case when LON_STATUS_FULL in ('Delinquent','Restrained','Written Off','Non-productive','Unknown')  then 1 else 0 end) as cnt_bad_loans,
# MAGIC 	sum(case when LON_STATUS_FULL in ('Delinquent','Restrained','Written Off','Non-productive','Unknown') then proceed_amt else 0 end) as vol_proceedamt_bad_loans,
# MAGIC 	sum(case when LON_STATUS_FULL in ('Delinquent','Restrained','Written Off','Non-productive','Unknown') then avg_bal else 0 end) as vol_avgbal_bad_loans,
# MAGIC 	
# MAGIC 	sum(case when lon_complet_f='Y' then 1 else 0 end) as cnt_lon_complete,
# MAGIC 	sum(case when lon_complet_f='Y' then proceed_amt else 0 end) as vol_proceedamt_lon_complete,
# MAGIC 	
# MAGIC 	max(issue_dt) as max_issue_dt, 
# MAGIC 	max(acount_reltnshp_last_maint_d) as max_acct_rel_last_maint,
# MAGIC 	avg(amortztn_months) as avg_amortztn_months
# MAGIC 	from
# MAGIC 		(select b.customr_num, a.account_key,a.lon_status, a.lon_complet_f, a.sic_c, a.proceed_amt, a.deposit_br_num,
# MAGIC 		a.issue_dt,a.os_amt,a.paymnt_typ,a.paymnt_amt ,a.partal_paymnt_amt , a.avg_bal, a.auth_amt, 
# MAGIC 		a.amortztn_months , a.tst, acount_reltnshp_last_maint_d, a.record_date,
# MAGIC 		substring(${last_date}, 1,6) as year_month,
# MAGIC 		CASE WHEN a.LON_STATUS = 'AC' THEN 'Active'
# MAGIC 		       WHEN a.LON_STATUS = 'CL' THEN 'Closed'
# MAGIC 		       WHEN a.LON_STATUS = 'DE' THEN 'Delinquent'
# MAGIC 		       WHEN a.LON_STATUS = 'RE' THEN 'Restrained'
# MAGIC 		       WHEN a.LON_STATUS = 'WO' THEN 'Written Off'
# MAGIC 		       WHEN a.LON_STATUS = 'NP' THEN 'Non-productive'
# MAGIC 		       ELSE 'Unknown' END AS LON_STATUS_full
# MAGIC 		from lon_loan_information a ----------only has last day of the month
# MAGIC 		left join
# MAGIC 		cif_customer_account_rel b
# MAGIC 		on a.record_date =b.record_date ---------merge on last day of the month
# MAGIC 		and a.account_key=b.account_key 
# MAGIC 		--where a.record_date=${last_date} ----------specify last business day of the month (do not need this because last day is in both tables)
# MAGIC 		)a
# MAGIC 	group by customr_num, year_month

# COMMAND ----------



# COMMAND ----------

# MAGIC %md ### account-cif mapping

# COMMAND ----------

## acct rel data
map = '''(select * from bbir.cif_customer_account_rel)t'''
df1 = spark.read.jdbc(url=bbirczJdbcUrl, table=map, properties=connectionProperties)
df1.createOrReplaceTempView('map')
df1.display(10)

# COMMAND ----------

sql_string_query = '''(SELECT * from INFORMATION_SCHEMA.TABLES)t'''
df3 = spark.read.jdbc(url=caedwczJdbcUrl, table=sql_string_query, properties=connectionProperties)
display(df3)

# COMMAND ----------

## edw acct data
acct = '''(select * from caedw.acct)t'''
df4 = spark.read.jdbc(url=caedwczJdbcUrl, table=acct, properties=connectionProperties)
df4.createOrReplaceTempView('acct')
df4.display(10)

# COMMAND ----------

## edw cust_acct data
ca = '''(select * from caedw.cust_acct)t'''
df4 = spark.read.jdbc(url=caedwczJdbcUrl, table=ca, properties=connectionProperties)
df4.createOrReplaceTempView('ca')
df4.display(10)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cif_business_customer where customr_num=4257 limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tmp_token_bridge where bbir_key_token='02ad11a36b6a3c7e0c2927b5e87110c22c7faa733cabd109dfef14e331ec5124'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cust where cust_id=214188000000953

# COMMAND ----------

get_sample_data(bbirczJdbcUrl,"bbir","cif_customer_account_rel").display()