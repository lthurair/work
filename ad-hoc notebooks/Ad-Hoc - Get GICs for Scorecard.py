# Databricks notebook source
# MAGIC %md
# MAGIC ********************************************************
# MAGIC **Purpose:** Get GIC counts based on scorecard eligibility for FY20 to FY24
# MAGIC
# MAGIC **Date:** Oct 16, 2024
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

# MAGIC %md #### Create temp bbir views

# COMMAND ----------

create_temp_table(bbirczJdbcUrl,"bbir","psa_eligible_sales",start_date=20191101, end_date=20241014)
create_temp_table(bbirczJdbcUrl,"bbir","hrm_employee_profile",start_date=20191101, end_date=20241014)


# COMMAND ----------

# MAGIC %md #### Get GIC data based on product codes from FY20 to FY24

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view sc as
# MAGIC select transaction_id, 
# MAGIC product_code, 
# MAGIC current_amount, 
# MAGIC recognition_date, 
# MAGIC pr_acf2_logon_id_credited_to as pr_acf2, 
# MAGIC cast(pr_branch_credited_to as int) as branch_credited, 
# MAGIC transaction_code, 
# MAGIC case when recognition_date between '2019-11-01' and '2020-10-31' then 'FY20'
# MAGIC when recognition_date between '2020-11-01' and '2021-10-31' then 'FY21'
# MAGIC when recognition_date between '2021-11-01' and '2022-10-31' then 'FY22'
# MAGIC when recognition_date between '2022-11-01' and '2023-10-31' then 'FY23'
# MAGIC when recognition_date between '2023-11-01' and '2024-10-15' then 'FY24'
# MAGIC end as fiscal_year,
# MAGIC ifw_effective_date
# MAGIC from psa_eligible_sales
# MAGIC where product_code in ('T008','T009','T043','T920','T921','T930','T943','T944','T990')
# MAGIC and (recognition_date between '2019-11-01' and '2024-10-15')

# COMMAND ----------

# MAGIC %md #### Add AMSB ACF2ID to GIC data and identify their start/end dates on the job

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view asd as
# MAGIC select *
# MAGIC from (
# MAGIC select acf2,row_number() over (partition by acf2 order by start_date desc) as rn, start_date, max_date, min_date
# MAGIC from (
# MAGIC select distinct 
# MAGIC trim(t1.ident_prty_idntftn_num) as acf2, date(t1.job_profile_start_date) as start_date, t2.max_date, t3.min_date
# MAGIC from hrm_employee_profile t1
# MAGIC inner join
# MAGIC     (select max(ifw_effective_date) as max_date, trim(ident_prty_idntftn_num) as acf2 
# MAGIC     from hrm_employee_profile
# MAGIC     where job_clasfcn_cd = 'W01446'
# MAGIC     group by trim(ident_prty_idntftn_num))t2
# MAGIC on trim(t1.ident_prty_idntftn_num)=t2.acf2
# MAGIC inner join
# MAGIC     (select min(ifw_effective_date) as min_date, trim(ident_prty_idntftn_num) as acf2 
# MAGIC     from hrm_employee_profile
# MAGIC     where job_clasfcn_cd = 'W01446'
# MAGIC     group by trim(ident_prty_idntftn_num))t3
# MAGIC on trim(t1.ident_prty_idntftn_num)=t3.acf2
# MAGIC where t1.job_clasfcn_cd = 'W01446'
# MAGIC )rc)v where rn=1

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view asd1 as
# MAGIC select acf2, coalesce(start_date, min_date) as beginn, max_date 
# MAGIC from asd

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from asd1 limit 4

# COMMAND ----------

# MAGIC %md #### Logic to identify if AMSB was recognized within their job start/end dates

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view ghj as
# MAGIC select a.*, 
# MAGIC case when a.recognition_date between b.beginn and b.max_date then 1 else 0 end as amsb_ind
# MAGIC from sc a
# MAGIC left join 
# MAGIC asd1 b
# MAGIC on a.pr_acf2=b.acf2
# MAGIC

# COMMAND ----------

df=spark.sql("select * from ghj")

# COMMAND ----------

# MAGIC %md #### Push via ADIDO OUT

# COMMAND ----------

adido_out(df,file_name="ADIDO-3667-gic_adhoc_sales_strategy",file_type='csv')

# COMMAND ----------

show_adido_out_content()