# Databricks notebook source
# MAGIC %md
# MAGIC ********************************************************
# MAGIC **Purpose:** Investigate Leads data issues
# MAGIC
# MAGIC **Date:** Oct 17, 2024
# MAGIC
# MAGIC ********************************************************

# COMMAND ----------

# MAGIC %md #### Set up Connections

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

# MAGIC %md #### Check unique Product Codes from Leads Data

# COMMAND ----------

sqlQuery = '''(select distinct
initve_prodct_target_cd, initve_prodct_target_ds
from caedw.leads 
where to_dt='9999-12-31'
and lead_source_cd=7332
and (ifw_effective_date between '20240401000000' and '20240729000000')
and initve_prodct_target_cd in ('3100','3101','3102','3103','3104','3139','3140','3195','3212','3213','3218','3223','3267','3272','7907','8907','8908','9984','9985','15726','7999','3216')
)t''' 
df12 = spark.read.jdbc(url=caedwczJdbcUrl,table=sqlQuery,properties=connectionProperties)
df12.createOrReplaceTempView("lead")
df12.display(10)

# COMMAND ----------

# MAGIC %md #### Get Leads Data

# COMMAND ----------

sqlQuery = '''(select lead_event_id, cust_id, asignd_to_branch_no, asignd_to_logon_id, lead_source_cd,
creatn_dt, closed_dt, lifecy_cd, lifecy_mn, lifecy_ds, lifecy_reason_cd, lifecy_reason_mn, lifecy_reason_ds, 
initve_prodct_target_cd, initve_prodct_target_ds, trim(upper(origtr_logon_id)) as orig_acf2, origtr_line_of_busnes_cd, origtr_line_of_busnes_mn, origtr_line_of_busnes_ds, origtr_branch_no, rcmndd_line_of_busnes_cd, rcmndd_line_of_busnes_mn, rcmndd_line_of_busnes_ds, recvng_line_of_busnes_cd, recvng_line_of_busnes_mn, recvng_line_of_busnes_ds, to_dt, ifw_effective_date
from caedw.leads 
where to_dt='9999-12-31'
and lead_source_cd=7332
and (ifw_effective_date between '20240401000000' and '20240729000000')
and initve_prodct_target_cd in ('3100','3101','3102','3103','3104','3139','3140','3195','3212','3213','3218','3223','3267','3272','7907','8907','8908','9984','9985','15726','7999','3216')
)t''' 
df12 = spark.read.jdbc(url=caedwczJdbcUrl,table=sqlQuery,properties=connectionProperties)
df12.createOrReplaceTempView("lead")
df12.display(10)

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view lead1 as
# MAGIC select *, concat(substring(ifw_effective_date, 1,4),substring(ifw_effective_date,5,2)) as mn
# MAGIC from lead

# COMMAND ----------

# MAGIC %md #### Create a temp view of leads_branch

# COMMAND ----------

sqlQuery = '''(select *
from caedw.leads_branch)t''' 
df12 = spark.read.jdbc(url=caedwczJdbcUrl,table=sqlQuery,properties=connectionProperties)
df12.createOrReplaceTempView("lead_branch")
df12.display(10)

# COMMAND ----------

# MAGIC %md #### Create temp views of bbir tables

# COMMAND ----------

create_temp_table(bbirczJdbcUrl,"bbir","hrm_employee_profile",start_date=20240401, end_date=20240729)
create_temp_table(bbirczJdbcUrl,"bbir","cif_business_customer",start_date=20240401, end_date=20240729)

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view bus as
# MAGIC select distinct cif_business_customer_key_token, customr_num, customr_name, substring(cast(record_date as varchar(8)),1,6) as mn
# MAGIC from cif_business_customer

# COMMAND ----------

sqlQuery = '''(select distinct bbir_key_token,caedw_id, substring(cast(bbir_record_date as varchar),1,6) as mn
from bbir_reports.bbir_caedw_token_bridge 
where bbir_aplictn_id='CIF'
and (bbir_record_date between 20240401 and 20240729))t''' 
df4 = spark.read.jdbc(url=bbirczJdbcUrl,table=sqlQuery,properties=connectionProperties)
df4.createOrReplaceTempView("token")
##df4.display(10)


# COMMAND ----------

# MAGIC %sql
# MAGIC     create or replace temporary view amsb_name as
# MAGIC     select distinct trim(upper(ident_prty_idntftn_num)) as acf2, 1 as SBB_ind, prefd_indiv_full_name, cc_intrnl_org_prty_id, substr(record_date,1,6) as mn
# MAGIC     from hrm_employee_profile hr
# MAGIC     where job_clasfcn_cd in ('W01446','W01476')
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view all_ret as
# MAGIC select distinct a.lead_event_id, a.cust_id, w.bbir_key_token, y.customr_num, y.customr_name,
# MAGIC a.asignd_to_branch_no, a.asignd_to_logon_id, a.lead_source_cd,
# MAGIC a.creatn_dt, a.closed_dt, a.lifecy_cd, a.lifecy_mn, a.lifecy_ds, a.lifecy_reason_cd, a.lifecy_reason_mn, a.lifecy_reason_ds, 
# MAGIC a.initve_prodct_target_cd, a.orig_acf2, a.origtr_branch_no, a.rcmndd_line_of_busnes_cd, a.rcmndd_line_of_busnes_mn, a.rcmndd_line_of_busnes_ds, a.to_dt, a.ifw_effective_date, 
# MAGIC b.branch_line_of_busnes_ds as new_orig_lob, c.branch_line_of_busnes_ds as new_assigned_lob, 
# MAGIC substring(a.ifw_effective_date,1,6) as mn,
# MAGIC k.prefd_indiv_full_name, k.cc_intrnl_org_prty_id as am_cc, SBB_ind
# MAGIC from lead1 a
# MAGIC left join 
# MAGIC lead_branch b 
# MAGIC on a.origtr_branch_no=b.branch_no
# MAGIC left join 
# MAGIC lead_branch c 
# MAGIC on a.asignd_to_branch_no=c.branch_no
# MAGIC inner join
# MAGIC amsb_name k
# MAGIC on a.orig_acf2=k.acf2
# MAGIC and a.mn=k.mn
# MAGIC left join
# MAGIC token w 
# MAGIC on a.cust_id=w.caedw_id
# MAGIC and a.mn=w.mn
# MAGIC left join 
# MAGIC bus y
# MAGIC on w.bbir_key_token=y.cif_business_customer_key_token
# MAGIC and w.mn=y.mn
# MAGIC --where a.ifw_effective_date>='20231101000000'
# MAGIC --and c.branch_line_of_busnes_ds in ('Small Business Banking','Commercial Banking','Small Business Banking Cash Management')
# MAGIC order by a.ifw_effective_date, lead_event_id
# MAGIC

# COMMAND ----------

spark.sql("CACHE TABLE all_ret")

# COMMAND ----------

# MAGIC %sql
# MAGIC select lead_event_id, count(*) as cnt 
# MAGIC from all_ret
# MAGIC group by lead_event_id
# MAGIC having count(*)>1 limit 2

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) as cnt from all_ret

# COMMAND ----------

# MAGIC %sql
# MAGIC create temporary view all_ret1 as
# MAGIC select * from lead

# COMMAND ----------

# MAGIC %md #### Push out

# COMMAND ----------

df5=spark.sql("select * from all_ret")
insert_to_synapse("analytics","all_ret",df5)

# COMMAND ----------

#df5=spark.sql("select * from all_ret1")
#publish_to_synapse("analytics","all_ret1",df5)