# Databricks notebook source
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

create_temp_table(bbirczJdbcUrl,"bbir","lon_business_facility",start_date="2024-07-01",end_date="2024-07-31")
create_temp_table(bbirczJdbcUrl,"bbir","lon_loan_information",start_date="2024-07-31",end_date="2024-07-31")
create_temp_table(bbirczJdbcUrl,"bbir","lon_business_account",start_date="2024-07-31",end_date="2024-07-31")
create_temp_table(bbirczJdbcUrl,"bbir","cif_business_customer",start_date="2024-07-31",end_date="2024-07-31")
create_temp_table(bbirczJdbcUrl,"bbir","cif_customer_account_rel",start_date="2024-07-31",end_date="2024-07-31")
create_temp_table(bbirczJdbcUrl,"bbir","cif_account_team_rel",start_date="2024-07-31",end_date="2024-07-31")create_temp_table(bbirczJdbcUrl,"bbir","bbdrc_branch_hierarchy",start_date="2024-07-31",end_date="2024-07-31")
