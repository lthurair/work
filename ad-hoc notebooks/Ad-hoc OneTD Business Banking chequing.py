# Databricks notebook source
# MAGIC %md
# MAGIC ********************************************************
# MAGIC **Purpose:** Provide Business Banking Chequing information, and Mapping between Business and Personal Customers
# MAGIC
# MAGIC **Date:** Oct 28, 2024
# MAGIC
# MAGIC **Transfer:** via AZ-AZ Combo Request
# MAGIC ********************************************************

# COMMAND ----------

# MAGIC %md ### Set up Connections

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run /Shared/Self_Migration_Jobs/Functions/Initialize_Connections

# COMMAND ----------

# MAGIC %run /Shared/Self_Migration_Jobs/Functions/Initialize_Functions

# COMMAND ----------

## edw tables - CZ
sql_cz_metadata = """(SELECT * FROM INFORMATION_SCHEMA.TABLES) t """
spark.read.jdbc(
    url=caedwczJdbcUrl, table=sql_cz_metadata, properties=connectionProperties
).display()

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

pd.set_option("display.width", 150)
pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   dga_final.armoured_cars
# MAGIC limit
# MAGIC   10

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   dga_final.final_tableau_wo_pia
# MAGIC where
# MAGIC   dt_yyyymm = '2024-07'
# MAGIC limit
# MAGIC   10 --summarize to scenarios

# COMMAND ----------

# MAGIC %md ### Create temp views for bbir tables

# COMMAND ----------

create_temp_table(bbirczJdbcUrl,"bbir","ntd_business_account",start_date="2024-07-01",end_date="2024-07-31")
create_temp_table(bbirczJdbcUrl,"bbir","cif_business_customer",start_date="2024-07-31",end_date="2024-07-31")

# COMMAND ----------

create_temp_table(bbirczJdbcUrl,"bbir","bbdrc_tst_ref")

# COMMAND ----------

create_temp_table(bbirczJdbcUrl,"bbir","bbdrc_common_ref")

# COMMAND ----------

sqlQuery = """(select *
from bbir_reports.bbir_caedw_token_bridge)t"""
df12 = spark.read.jdbc(url=bbirczJdbcUrl, table=sqlQuery, properties=connectionProperties)
df12.createOrReplaceTempView("token")
df12.display(10)

# COMMAND ----------

# MAGIC %md ### (1) OneTD table contains cust_id, customr_since (Lakshmi's table)

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view descr as
# MAGIC select distinct
# MAGIC   cust_id,
# MAGIC   pers_cust_id,
# MAGIC   customr_since_d,
# MAGIC   to_date(concat(cast(customr_since_d as string), '01'),"yyyyMMdd") as customer_since,
# MAGIC   lob,
# MAGIC   segment,
# MAGIC   cif_business_customer_key,
# MAGIC   rpt_Dt
# MAGIC from
# MAGIC   dga_final.one_td
# MAGIC where
# MAGIC   rpt_Dt = 202407

# COMMAND ----------

# MAGIC %md ### (2) Get Money in/out Balances

# COMMAND ----------

sqlQuery = """(select cust_id, 
efectv_dt, 
mnyin_active_prtbal_am, 
mnyout_active_prtbal_am
from caedw.cust_basic_sumary 
where efectv_dt='2024-07-31')t"""
df12 = spark.read.jdbc(url=caedwczJdbcUrl, table=sqlQuery, properties=connectionProperties)
df12.createOrReplaceTempView("cust_sum")
df12.display(10)

# COMMAND ----------

# MAGIC %md ### (3) Get Chequing Account info

# COMMAND ----------

# MAGIC %md ####(3a) Get all deposit products excluding chequing and savings

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view cif_das7 as
# MAGIC select distinct 
# MAGIC cif_business_customer_key,
# MAGIC 1 as dep_flag
# MAGIC from dga_final.j0001m_deposit
# MAGIC where record_date = 20240731
# MAGIC and acount_status_c <> 'C'
# MAGIC and (plan_name not in ('Unlimited','Basic','Everyday A','Everyday B','Everyday C','Community','Community Plus') or product_c not in ('22','40','42','44','46','56'))

# COMMAND ----------

# MAGIC %md ####(3b) Get chequing account flag

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view cif_das0 as
# MAGIC select distinct 
# MAGIC cif_business_customer_key,
# MAGIC  'Y' as has_business_chequing_account1,
# MAGIC 1 as chequing_flag
# MAGIC from dga_final.j0001m_deposit
# MAGIC where record_date = 20240731
# MAGIC and acount_status_c <> 'C'
# MAGIC and plan_name in ('Unlimited','Basic','Everyday A','Everyday B','Everyday C','Community','Community Plus')

# COMMAND ----------

# MAGIC %md ####(3c) Get savings account flag

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view cif_das1 as
# MAGIC select distinct
# MAGIC cif_business_customer_key,
# MAGIC 'Y' as has_business_savings_account1,
# MAGIC 1 as savings_flag
# MAGIC from dga_final.j0001m_deposit
# MAGIC where record_date = 20240731
# MAGIC and acount_status_c <> 'C'
# MAGIC and product_c in ('22','40','42','44','46','56')

# COMMAND ----------

# MAGIC %md ### (4) Get counts of all other products

# COMMAND ----------

# MAGIC %md #### (4a) Get distinct NTD products

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view ntd_cnt as
# MAGIC select distinct
# MAGIC   cif_business_customer_key_token,
# MAGIC   1 as ntd_count
# MAGIC from
# MAGIC   dga_final.j0001m_term
# MAGIC where
# MAGIC   record_date = 20240731

# COMMAND ----------

# MAGIC %md #### (4b) Get distinct loans

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view lon_all as
# MAGIC select
# MAGIC   cif_business_customer_key_token,
# MAGIC   count(distinct(product_type)) as distinct_lon_cnt
# MAGIC from
# MAGIC   dga_final.j0001m_credit
# MAGIC where
# MAGIC   lon_status <> 'CL'
# MAGIC   and record_date = 20240731
# MAGIC group by
# MAGIC   cif_business_customer_key_token

# COMMAND ----------

# MAGIC %md #### (4c) Get distinct Merchant products and OBSV

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view mer_obsv as
# MAGIC select
# MAGIC   cif_business_customer_key,
# MAGIC   case when MSM_Products_list>0 then 1 else 0 end as merchant_cnt,
# MAGIC   case when OBSV_Products_list>0 then 1 else 0 end as obsv_cnt
# MAGIC   --coalesce(MSM_Products_list, 0) as merchant_cnt,
# MAGIC   --coalesce(OBSV_Products_list, 0) as obsv_cnt
# MAGIC from
# MAGIC   dga_final.LT_ad_hoc_count

# COMMAND ----------

# MAGIC %md ### (5) Join all the views: BB cust_id's, DAS, NTD, LON, Mer/OBSV

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view ad_hoc as
# MAGIC select distinct
# MAGIC   a.cif_business_customer_key,
# MAGIC  -- x.customr_name,
# MAGIC   --x.cif_business_customer_key_token,
# MAGIC   a.cust_id,
# MAGIC   a.pers_cust_id,
# MAGIC   '' as service_plan_name,
# MAGIC   c.has_business_chequing_account1,
# MAGIC   b.has_business_savings_account1,
# MAGIC
# MAGIC   coalesce(u.dep_flag,0) as dep,
# MAGIC   coalesce(c.chequing_flag,0) as cheq,
# MAGIC   coalesce(b.savings_flag,0) as sav,
# MAGIC   coalesce(e.ntd_count,0) as ntd_cnt1,
# MAGIC   coalesce(f.distinct_lon_cnt,0) as lon_cnt,
# MAGIC   coalesce(g.merchant_cnt,0) as mer_cnt,
# MAGIC   coalesce(g.obsv_cnt,0) as obsv_cnt1,
# MAGIC
# MAGIC   d.mnyin_active_prtbal_am as money_in_bal,
# MAGIC   d.mnyout_active_prtbal_am as money_out_bal,
# MAGIC   round(months_between("2024-07-31", customer_since), 2) as tenure
# MAGIC from
# MAGIC   descr a
# MAGIC   left join 
# MAGIC   cif_das7 u 
# MAGIC   on a.cif_business_customer_key=u.cif_business_customer_key
# MAGIC   left join
# MAGIC   cif_das1 b 
# MAGIC   on a.cif_business_customer_key=b.cif_business_customer_key 
# MAGIC   left join 
# MAGIC   cif_das0 c
# MAGIC   on a.cif_business_customer_key=c.cif_business_customer_key
# MAGIC   left join
# MAGIC   cif_business_customer x 
# MAGIC   on a.cif_business_customer_key = x.cif_business_customer_key
# MAGIC   left join 
# MAGIC   cust_sum d
# MAGIC   on a.cust_id = d.cust_id
# MAGIC   left join 
# MAGIC   ntd_cnt e 
# MAGIC   on x.cif_business_customer_key_token = e.cif_business_customer_key_token
# MAGIC   left join 
# MAGIC   lon_all f 
# MAGIC   on x.cif_business_customer_key_token = f.cif_business_customer_key_token
# MAGIC   left join 
# MAGIC   mer_obsv g 
# MAGIC   on a.cif_business_customer_key = g.cif_business_customer_key

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ad_hoc limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view ad_hoc_final as
# MAGIC select
# MAGIC cust_id, 
# MAGIC pers_cust_id,
# MAGIC service_plan_name,
# MAGIC
# MAGIC case when has_business_chequing_account1 is null then 'N' else has_business_chequing_account1 end as has_business_chequing_account,
# MAGIC
# MAGIC case when has_business_savings_account1 is null then 'N' else has_business_savings_account1 end as has_business_savings_account,
# MAGIC
# MAGIC 'N/A' as business_cheq_impacted,
# MAGIC cheq+sav+ntd_cnt1+lon_cnt+mer_cnt+obsv_cnt1 as number_products,
# MAGIC money_in_bal,
# MAGIC money_out_bal,
# MAGIC tenure
# MAGIC from ad_hoc

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ad_hoc_final where number_products=0 limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select cust_id, count(*) as cnt from
# MAGIC (select distinct cust_id, service_plan_name,
# MAGIC has_business_chequing_account,
# MAGIC has_business_savings_account,
# MAGIC business_cheq_impacted,
# MAGIC number_products,
# MAGIC money_in_bal,
# MAGIC money_out_bal,
# MAGIC tenure
# MAGIC from ad_hoc_final)a
# MAGIC group by cust_id, service_plan_name,
# MAGIC has_business_chequing_account,
# MAGIC has_business_savings_account,
# MAGIC business_cheq_impacted,
# MAGIC number_products,
# MAGIC money_in_bal,
# MAGIC money_out_bal,
# MAGIC tenure 
# MAGIC having count(*)>1
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md ### (6) Push to ADIDO-OUT (OUTPUT OF CSV FILE)

# COMMAND ----------

df=spark.sql("select * from ad_hoc_final")

# COMMAND ----------

adido_out(df,file_name="ADIDO-3692-BusinessBanking",file_type='csv')

# COMMAND ----------

show_adido_out_content()