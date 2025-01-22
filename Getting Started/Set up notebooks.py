# Databricks notebook source
# MAGIC %md ### Reset and set up connections

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

sql_srz_metadata = '''(SELECT * FROM INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA='bbcrm')t '''
spark.read.jdbc(url=srzJdbcUrl,table=sql_srz_metadata,properties=connectionProperties).display()

# COMMAND ----------

# MAGIC %md ### Start Development

# COMMAND ----------

