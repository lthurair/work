# Databricks notebook source
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

create_temp_table(bbirczJdbcUrl,"bbir","cif_business_customer",end_date=20241120)
create_temp_table(bbirczJdbcUrl,"bbir","bif_branch_information",end_date=20241120)
create_temp_table(bbirczJdbcUrl,"bbir","lon_business_facility",end_date=20241120)
create_temp_table(bbirczJdbcUrl,"bbir","lon_business_account",end_date=20241120)
create_temp_table(bbirczJdbcUrl,"bbir","cif_customer_account_rel",end_date=20241120)
create_temp_table(bbirczJdbcUrl,"bbir","cif_account_team_rel",end_date=20241120)
create_temp_table(bbirczJdbcUrl,"bbir","lon_loan_information",end_date=20241120)


# COMMAND ----------

sqlQuery = '''(select *
from bbir.bbdrc_branch_hierarchy)t''' 
df4 = spark.read.jdbc(url=bbirczJdbcUrl,table=sqlQuery,properties=connectionProperties)
df4.createOrReplaceTempView("bbdrc_branch_hierarchy")

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct * from bbdrc_branch_hierarchy where branch = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct os_amt from lon_loan_information limit 30