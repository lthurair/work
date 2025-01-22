# Databricks notebook source
# MAGIC %md
# MAGIC ********************************************************
# MAGIC **Purpose:** Get counts of Active SBB Customers who are also with Private Banking
# MAGIC
# MAGIC **Date:** Oct 23, 2024
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

# MAGIC %md #### Create temp bbir view

# COMMAND ----------

create_temp_table(bbirczJdbcUrl,"bbir","cif_business_customer",end_date=20241021)
create_temp_table(bbirczJdbcUrl,"bbir","bbdrc_common_ref")

# COMMAND ----------

# MAGIC %md #### Check what values exist in the bbdrc_common_ref view

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bbdrc_common_ref where table_name='CIF_BUSINESS_CUSTOMER' and field_name='CUSTOMR_STATUS'

# COMMAND ----------

# MAGIC %md #### Identify the PB customers

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view ghj as
# MAGIC select distinct customr_num, cif_business_customer_key, customr_name,
# MAGIC case when specl_customr_f01='PRI' or specl_customr_f02='PRI' or specl_customr_f03='PRI' or specl_customr_f04='PRI' then 1 else 0 end as pb_ind, ifw_effective_date
# MAGIC from cif_business_customer 
# MAGIC where primary_segment='SBB'
# MAGIC and customr_status=00 --NORMAL CUSTOMER

# COMMAND ----------

# MAGIC %md #### Calculate percentage of PB customers over total BB customers

# COMMAND ----------

# MAGIC %sql
# MAGIC select ((count(case when pb_ind=1 then pb_ind end))/(count(*)))*100
# MAGIC from ghj