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

sqlQuery = '''(select id, llc_bi__account__c, llc_bi__covenant2__c, name, createdbyid, ifw_effective_date
from bbcrm.llc_bi__account_covenant__c_view)t''' 
df3 = spark.read.jdbc(url=srzJdbcUrl,table=sqlQuery,properties=connectionProperties)
df3.createOrReplaceTempView("acct_cov")
df3.display(10)
#llc_bi__account__c

# COMMAND ----------

sqlQuery = '''(select id, 
llc_bi__account__c, 
llc_bi__active__c,
category__c, 
name, 
llc_bi__covenant_type__c, 
createdbyid, 
llc_bi__frequency__c, 
full_name__c,
llc_bi__last_evaluation_status__c, 
llc_bi__last_evaluation_date__c,
llc_bi__financial_indicator_value__c,
ifw_effective_date, 
ifw_partition_date
from bbcrm.llc_bi__covenant2__c_view)t''' 
df0 = spark.read.jdbc(url=srzJdbcUrl,table=sqlQuery,properties=connectionProperties)
df0.createOrReplaceTempView("covenant_mgmt")
df0.display(10)

# COMMAND ----------

sqlQuery = '''(select id, 
category__c, 
covenant_type__c, 
name, 
llc_bi__covenant__c, 
llc_bi__evaluated_by__c, 
llc_bi__evaluation_date__c, 
llc_bi__status__c, 
financial_indicator_result__c, 
default_letter_issued__c, 
default_letter_issued_date__c,
ifw_effective_date, 
ifw_partition_date 
from bbcrm.llc_bi__covenant_compliance2__c_view)t''' 
df2 = spark.read.jdbc(url=srzJdbcUrl,table=sqlQuery,properties=connectionProperties)
df2.createOrReplaceTempView("comp")
df2.display(10)

# COMMAND ----------

sqlQuery = '''(select id,
llc_bi__category__c,
name,
full_name__c,
createdbyid,
ifw_effective_date,
ifw_partition_date
from bbcrm.llc_bi__covenant_type__c_view)t''' 
df1 = spark.read.jdbc(url=srzJdbcUrl,table=sqlQuery,properties=connectionProperties)
df1.createOrReplaceTempView("cov_type")
df1.display(10)

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table dga_final.bbcrm_account_covenant_base as
# MAGIC --create or replace temporary view base as
# MAGIC select a.id as acct_cov_id, 
# MAGIC a.llc_bi__account__c as acct_cov_bi_account_id,
# MAGIC a.llc_bi__covenant2__c,
# MAGIC a.name as acct_cov_name, 
# MAGIC a.createdbyid as acct_cov_createdbyid, 
# MAGIC a.ifw_effective_date as acct_cov_eff_date,
# MAGIC
# MAGIC b.id as cov_mgmt_id, 
# MAGIC b.llc_bi__account__c as cov_mgmt_bi_account_id, 
# MAGIC b.llc_bi__active__c as cov_active_status,
# MAGIC b.category__c as cov_mgmt_category, 
# MAGIC b.name as cov_mgmt_name, 
# MAGIC --b.llc_bi__covenant_type__c, --same as 'id' on covenant type object
# MAGIC b.createdbyid as cov_mgmt_createdbyid, 
# MAGIC b.llc_bi__frequency__c as reporting_frequency, 
# MAGIC b.full_name__c as cov_mgmt_fullname,
# MAGIC b.llc_bi__last_evaluation_status__c as out_of_compliance_status, 
# MAGIC b.llc_bi__last_evaluation_date__c as out_of_compliance_date,
# MAGIC b.llc_bi__financial_indicator_value__c as test_requirement_status,
# MAGIC
# MAGIC c.id as cov_compliance_id, 
# MAGIC c.category__c as cov_compliance_category, 
# MAGIC c.covenant_type__c as covenant_type, 
# MAGIC c.name as cov_compliance_name, 
# MAGIC --c.llc_bi__covenant__c, --same as 'id' on covenant management
# MAGIC c.llc_bi__evaluated_by__c as evaluated_by, 
# MAGIC c.llc_bi__evaluation_date__c as evaluated_date, 
# MAGIC c.llc_bi__status__c as cov_compliance_status, 
# MAGIC c.financial_indicator_result__c as compliance_result, 
# MAGIC c.default_letter_issued__c as default_letter_issued, 
# MAGIC c.default_letter_issued_date__c as default_letter_issued_date,
# MAGIC
# MAGIC d.id as cov_type_id,
# MAGIC d.llc_bi__category__c as cov_category,
# MAGIC d.name as cov_type_name,
# MAGIC d.full_name__c as cov_type_fullname,
# MAGIC d.createdbyid as cov_type_createdbyid
# MAGIC
# MAGIC from acct_cov a ----------------------------------account covenant
# MAGIC join
# MAGIC covenant_mgmt b ----------------------------------covenant management
# MAGIC on a.llc_bi__covenant2__c=b.id
# MAGIC --on a.llc_bi__account__c=b.llc_bi__account__c
# MAGIC join
# MAGIC comp c -------------------------------------------covenant compliance
# MAGIC on b.id=c.llc_bi__covenant__c
# MAGIC join
# MAGIC cov_type d ---------------------------------------covenant type
# MAGIC on b.llc_bi__covenant_type__c=d.id
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC --llc_bi__covenant2__c,count(*) as cnt 
# MAGIC from base limit 3
# MAGIC --where llc_bi__covenant2__c='a2O8V000007bQJZUA2'
# MAGIC --group by llc_bi__covenant2__c
# MAGIC --having count(*)>1