# Databricks notebook source
# MAGIC %md ## Set up connections

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
import datetime
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

## BBCRM and EXC tables - SRZ
sql_srz_metadata = '''(SELECT * FROM INFORMATION_SCHEMA.TABLES where table_schema='bbcrm') t '''
spark.read.jdbc(url=srzJdbcUrl,table=sql_srz_metadata,properties=connectionProperties).display()

# COMMAND ----------

# MAGIC %md ### Create the Account-Covenant Base Table

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view acct_cov as
# MAGIC select ID, LLC_BI__ACCOUNT__C, LLC_BI__COVENANT2__C, NAME, CREATEDBYID
# MAGIC from bbart_test_cycle2.llc_bi__account_covenant__c
# MAGIC --llc_bi__account__c
# MAGIC --id, llc_bi__account__c, llc_bi__covenant2__c, name, createdbyid, ifw_effective_date

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from acct_cov

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW covenant_mgmt as
# MAGIC select ID
# MAGIC ,LLC_BI__ACCOUNT__C 
# MAGIC ,LLC_BI__ACTIVE__C
# MAGIC ,CATEGORY__C 
# MAGIC ,NAME 
# MAGIC ,LLC_BI__COVENANT_TYPE__C
# MAGIC ,CREATEDBYID 
# MAGIC ,LLC_BI__FREQUENCY__C 
# MAGIC ,FULL_NAME__C
# MAGIC ,LLC_BI__LAST_EVALUATION_STATUS__C 
# MAGIC ,LLC_BI__LAST_EVALUATION_DATE__C
# MAGIC ,LLC_BI__FINANCIAL_INDICATOR_VALUE__C
# MAGIC --,IFW_EFECTIVE_DATE 
# MAGIC --,IFW_PARTITION_DATE
# MAGIC from bbart_test_cycle2.llc_bi__covenant2__c

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view comp as
# MAGIC select ID 
# MAGIC ,CATEGORY__C 
# MAGIC ,COVENANT_TYPE__C 
# MAGIC ,NAME 
# MAGIC ,LLC_BI__COVENANT__C 
# MAGIC ,LLC_BI__EVALUATED_BY__C 
# MAGIC ,LLC_BI__EVALUATION_DATE__C 
# MAGIC ,LLC_BI__STATUS__C 
# MAGIC ,FINANCIAL_INDICATOR_RESULT__C 
# MAGIC ,DEFAULT_LETTER_ISSUED__C 
# MAGIC ,DEFAULT_LETTER_ISSUED_DATE__C
# MAGIC from bbart_test_cycle2.llc_bi__covenant_compliance2__c

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from comp

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view cov_type as
# MAGIC select ID
# MAGIC ,LLC_BI__CATEGORY__C
# MAGIC ,NAME
# MAGIC ,FULL_NAME__C
# MAGIC ,CREATEDBYID
# MAGIC from bbart_test_cycle2.llc_bi__covenant_type__c

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from cov_type

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table dga_final.test_cycle2_bbcrm_account_covenant_base as
# MAGIC select a.ID as acct_cov_id, 
# MAGIC a.LLC_BI__ACCOUNT__C as acct_cov_bi_account_id,
# MAGIC a.LLC_BI__COVENANT2__C,
# MAGIC a.NAME as acct_cov_name, 
# MAGIC a.CREATEDBYID as acct_cov_createdbyid, 
# MAGIC '' as acct_cov_eff_date,
# MAGIC
# MAGIC b.ID as cov_mgmt_id, 
# MAGIC b.LLC_BI__ACCOUNT__C as cov_mgmt_bi_account_id, 
# MAGIC b.LLC_BI__ACTIVE__C as cov_active_status,
# MAGIC b.CATEGORY__C as cov_mgmt_category, 
# MAGIC b.NAME as cov_mgmt_name, 
# MAGIC --b.llc_bi__covenant_type__c, --same as 'id' on covenant type object
# MAGIC b.CREATEDBYID as cov_mgmt_createdbyid, 
# MAGIC b.LLC_BI__FREQUENCY__C as reporting_frequency, 
# MAGIC b.FULL_NAME__C as cov_mgmt_fullname,
# MAGIC b.LLC_BI__LAST_EVALUATION_STATUS__C as out_of_compliance_status, 
# MAGIC b.LLC_BI__LAST_EVALUATION_DATE__C as out_of_compliance_date,
# MAGIC b.LLC_BI__FINANCIAL_INDICATOR_VALUE__C as test_requirement_status,
# MAGIC
# MAGIC c.ID as cov_compliance_id, 
# MAGIC c.CATEGORY__C as cov_compliance_category, 
# MAGIC c.COVENANT_TYPE__C as covenant_type, 
# MAGIC c.NAME as cov_compliance_name, 
# MAGIC c.LLC_BI__EVALUATED_BY__C as evaluated_by, 
# MAGIC c.LLC_BI__EVALUATION_DATE__C as evaluated_date, 
# MAGIC c.LLC_BI__STATUS__C as cov_compliance_status, 
# MAGIC c.FINANCIAL_INDICATOR_RESULT__C as compliance_result, 
# MAGIC c.DEFAULT_LETTER_ISSUED__C as default_letter_issued, 
# MAGIC c.DEFAULT_LETTER_ISSUED_DATE__C as default_letter_issued_date,
# MAGIC
# MAGIC d.ID as cov_type_id,
# MAGIC d.LLC_BI__CATEGORY__C as cov_category,
# MAGIC d.NAME as cov_type_name,
# MAGIC d.FULL_NAME__C as cov_type_fullname,
# MAGIC d.CREATEDBYID as cov_type_createdbyid
# MAGIC
# MAGIC from acct_cov a ----------------------------------account covenant
# MAGIC join
# MAGIC covenant_mgmt b ----------------------------------covenant management
# MAGIC on a.LLC_BI__COVENANT2__C=b.ID
# MAGIC --on a.llc_bi__account__c=b.llc_bi__account__c
# MAGIC join
# MAGIC comp c -------------------------------------------covenant compliance
# MAGIC on b.ID=c.LLC_BI__COVENANT__C
# MAGIC join
# MAGIC cov_type d ---------------------------------------covenant type
# MAGIC on b.LLC_BI__COVENANT_TYPE__C=d.ID
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dga_final.test_cycle2_bbcrm_account_covenant_base