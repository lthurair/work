# Databricks notebook source
# MAGIC %md # Below are some common syntax that can be useful for your development

# COMMAND ----------

# MAGIC %md #### Restart Python

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md #### Start Connection

# COMMAND ----------

# MAGIC %run /Shared/Self_Migration_Jobs/Functions/Initialize_Connections

# COMMAND ----------

# MAGIC %md #### Initialize Functions

# COMMAND ----------

# MAGIC %run /Shared/Self_Migration_Jobs/Functions/Initialize_Functions

# COMMAND ----------

# MAGIC %md #### Import Python/Pyspark libraries

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

# MAGIC %md #### See what tables you have access to in SRZ; you can also modify the 'where' clause and 'url' to 'bbircz' to see what tables you have permission in BBIR

# COMMAND ----------

## BBCRM tables - SRZ
sql_srz_metadata = '''(SELECT * FROM INFORMATION_SCHEMA.TABLES where table_schema='bbcrm') t '''
spark.read.jdbc(url=srzJdbcUrl,table=sql_srz_metadata,properties=connectionProperties).display()

# COMMAND ----------

# MAGIC %md #### Query a BBCRM table (account covenant) and create a temp view called 'acct_cov'

# COMMAND ----------

sqlQuery = '''(select id, llc_bi__account__c, llc_bi__covenant2__c, name, createdbyid, ifw_effective_date
from bbcrm.llc_bi__account_covenant__c_view)t''' 
df3 = spark.read.jdbc(url=srzJdbcUrl,table=sqlQuery,properties=connectionProperties)
df3.createOrReplaceTempView("acct_cov")
df3.display(10)

# COMMAND ----------

# MAGIC %md #### Write a SQL query and create a temp view; can use this to also join multiple temp views together. Instead of a temp view, you can also create permanent tables, but this is only suggested once you have your final output. Final permanent tables can be placed in the schema/database called dga_final

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view example_view as
# MAGIC select * 
# MAGIC from acct_cov limit 10

# COMMAND ----------

# MAGIC %md ### Below are some useful syntax to push a table/view to synapse. This is needed when you want to ADIDO OUT your final output.

# COMMAND ----------

# MAGIC %md #### (1) Create a dataframe based on your table/view

# COMMAND ----------

df=spark.sql("select * from example_view")

# COMMAND ----------

# MAGIC %md #### (2) Push to Synapse (this is for ADIDO OUT). Only do this once the PIA has been approved and the ADIDO OUT intake form has been approved.

# COMMAND ----------

adido_out(df,file_name="ADIDO-jira#-name_of_file",file_type='csv')

# COMMAND ----------

# MAGIC %md #### (3) Below will show if your file has gone through

# COMMAND ----------

show_adido_out_content()