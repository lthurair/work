# Databricks notebook source
# MAGIC %md ## Set up Connections

# COMMAND ----------

# MAGIC %run /Shared/Self_Migration_Jobs/Functions/Initialize_Connections

# COMMAND ----------

# MAGIC %run /Shared/Self_Migration_Jobs/Functions/Initialize_Functions

# COMMAND ----------

# MAGIC %md ### List all tables in data-in folder

# COMMAND ----------

dbutils.fs.ls("abfss://ca00bbacsm0001@edaaaedle1prdadido.dfs.core.windows.net/data-in/CA00BBACSM0001/thural2")

# COMMAND ----------

# MAGIC %md ### Create Schema (Database)

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists crm_esit;
# MAGIC --drop schema crm_esit
# MAGIC --show tables in crm_esit

# COMMAND ----------

# MAGIC %md ### (1) Account File

# COMMAND ----------

# adido-in the files
df = adido_in('ADIDO-476-LT_Account', ',')
df.display()

# COMMAND ----------

# MAGIC %md #### save dataframe into table

# COMMAND ----------

df.write.format("delta").mode("overwrite").saveAsTable("crm_esit.Account")

# COMMAND ----------

# MAGIC %md #### read table

# COMMAND ----------

spark.sql(f"select * from crm_esit.Account limit 10").display()

# COMMAND ----------

# MAGIC %md ### (2) CurrencyType File

# COMMAND ----------

df1=adido_in('ADIDO-476-LT_CurrencyType',',')

# COMMAND ----------

# MAGIC %md ####Save dataframe into table

# COMMAND ----------

df1.write.format("delta").mode("overwrite").saveAsTable("crm_esit.CurrencyType")

# COMMAND ----------

# MAGIC %md #### Display data

# COMMAND ----------

df1.display()

# COMMAND ----------

# MAGIC %md ### (3) LLC_BI_Account_Collateral File

# COMMAND ----------

## adido-in file
df3=adido_in('ADIDO-476-LT_LLC_BI_Account_Collateral_c',',')

# COMMAND ----------

# MAGIC %md #### Save into Table

# COMMAND ----------

df3.write.format("delta").mode("overwrite").saveAsTable("crm_esit.LLC_BI_Account_Collateral_c")

# COMMAND ----------

# MAGIC %md #### Display first 5 records

# COMMAND ----------

df3.show(5)

# COMMAND ----------

# MAGIC %md ### (4) LLC_BI_Collateral_Type_c File

# COMMAND ----------

# MAGIC %md #### Adido-in file

# COMMAND ----------

df4=adido_in('ADIDO-476-LT_LLC_BI_Collateral_Type_c',',')

# COMMAND ----------

# MAGIC %md #### Save into Table

# COMMAND ----------

df4.write.format("delta").mode("overwrite").saveAsTable("crm_esit.LLC_BI_Collateral_Type_c")

# COMMAND ----------

# MAGIC %md #### Display 5 records

# COMMAND ----------

df4.show(5)

# COMMAND ----------

# MAGIC %md ### (5) LLC_BI_Collateral_c File

# COMMAND ----------

# MAGIC %md #### Adido-in File

# COMMAND ----------

df5=adido_in('ADIDO-476-LT_LLC_BI_Collateral_c',',')

# COMMAND ----------

# MAGIC %md #### Save into table

# COMMAND ----------

df5.write.format("delta").mode("overwrite").saveAsTable("crm_esit.LLC_BI_Collateral_c")

# COMMAND ----------

# MAGIC %md ### (6) LLC_BI_Connection_Role_c File

# COMMAND ----------

# MAGIC %md #### Adido-in File

# COMMAND ----------

df6=adido_in('ADIDO-476-LT_LLC_BI_Connection_Role_c',',')

# COMMAND ----------

# MAGIC %md #### Save dataframe into table

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop table crm_esit.LLC_BI_Connection_Role_c

# COMMAND ----------

df6.write.format("delta").mode("overwrite").saveAsTable("crm_esit.LLC_BI_Connection_Role_c")

# COMMAND ----------

# MAGIC %md #### Display first 5 records

# COMMAND ----------

df6.show(5)

# COMMAND ----------

df6.display(5)

# COMMAND ----------

# MAGIC %md ### (7) LLC_BI_Connection_c File

# COMMAND ----------

# MAGIC %md #### Adido-in file

# COMMAND ----------

df7=adido_in('ADIDO-476-LT_LLC_BI_Connection_c',',')

# COMMAND ----------

# MAGIC %md #### Save dataframe in table

# COMMAND ----------

df7.write.format("delta").mode("overwrite").saveAsTable("crm_esit.LLC_BI_Connection_c")

# COMMAND ----------

# MAGIC %md #### Display first 5 records

# COMMAND ----------

df7.display(5)

# COMMAND ----------

# MAGIC %md ### (8) LLC_BI_Covenant2_c File

# COMMAND ----------

# MAGIC %md #### Adido-in file

# COMMAND ----------

df8=adido_in('ADIDO-476-LT_LLC_BI_Covenant2_c',',')

# COMMAND ----------

# MAGIC %md #### Save dataframe into table

# COMMAND ----------

df8.write.format("delta").mode("overwrite").saveAsTable("crm_esit.LLC_BI_Covenant2_c")

# COMMAND ----------

# MAGIC %md #### Display records

# COMMAND ----------

df8.display(5)

# COMMAND ----------

# MAGIC %md ### (9) LLC_BI_Covenant_Compliance2_c File

# COMMAND ----------

# MAGIC %md #### adido-in file

# COMMAND ----------

df9=adido_in('ADIDO-476-LT_LLC_BI_Covenant_Compliance2_c',',')

# COMMAND ----------

# MAGIC %md #### Save file into table

# COMMAND ----------

df9.write.format("delta").mode("overwrite").saveAsTable("crm_esit.LLC_BI_Covenant_Compliance2_c")

# COMMAND ----------

# MAGIC %md #### Display records

# COMMAND ----------

df9.display(5)

# COMMAND ----------

# MAGIC %md ### (10) LLC_BI_Legal_Entities_c File

# COMMAND ----------

# MAGIC %md #### adido in file

# COMMAND ----------

df10=adido_in('ADIDO-476-LT_LLC_BI_Legal_Entities_c',',')

# COMMAND ----------

# MAGIC %md #### save in a table

# COMMAND ----------

df10.write.format("delta").mode("overwrite").saveAsTable("crm_esit.LLC_BI_Legal_Entities_c")

# COMMAND ----------

# MAGIC %md #### display records

# COMMAND ----------

df10.display(5)

# COMMAND ----------

# MAGIC %md ### (11) LLC_BI_Loan_Collateral2_c

# COMMAND ----------

# MAGIC %md #### Adido-in file

# COMMAND ----------

df11=adido_in('ADIDO-476-LT_LLC_BI_Loan_Collateral2_c',',')

# COMMAND ----------

# MAGIC %md #### Save into table

# COMMAND ----------

df11.write.format("delta").mode("overwrite").saveAsTable("crm_esit.LLC_BI_Loan_Collateral2_c")

# COMMAND ----------

# MAGIC %md #### read table and display records

# COMMAND ----------

spark.sql(f"select * from crm_esit.LLC_BI_Loan_Collateral2_c limit 10").display(5)

# COMMAND ----------

# MAGIC %md ### (12) LLC_BI_Loan_c File

# COMMAND ----------

# MAGIC %md ####adido-in file

# COMMAND ----------

df12=adido_in('ADIDO-476-LT_LLC_BI_Loan_c',',')

# COMMAND ----------

# MAGIC %md #### Save into table

# COMMAND ----------

df12.write.format("delta").mode("overwrite").saveAsTable("crm_esit.LLC_BI_Loan_c")

# COMMAND ----------

# MAGIC %md #### read table

# COMMAND ----------

spark.sql(f"select * from crm_esit.LLC_BI_Loan_c limit 5").display()

# COMMAND ----------

# MAGIC %md ### (13) LLC_BI_Policy_Exception_Template_c File

# COMMAND ----------

# MAGIC %md #### adido-in file

# COMMAND ----------

df13=adido_in('ADIDO-476-LT_LLC_BI_Policy_Exception_Template_c',',')

# COMMAND ----------

# MAGIC %md #### save dataframe into table

# COMMAND ----------

df13.write.format("delta").mode("overwrite").saveAsTable("crm_esit.LLC_BI_Policy_Exception_Template_c")

# COMMAND ----------

# MAGIC %md #### read table

# COMMAND ----------

spark.sql(f"select * from crm_esit.LLC_BI_Policy_Exception_Template_c limit 5").display()

# COMMAND ----------

# MAGIC %md ### (14) LLC_BI_Policy_Exception_c File

# COMMAND ----------

# MAGIC %md #### adido-in file

# COMMAND ----------

df14=adido_in('ADIDO-476-LT_LLC_BI_Policy_Exception_c',',')

# COMMAND ----------

# MAGIC %md #### save into table

# COMMAND ----------

df14.write.format("delta").mode("overwrite").saveAsTable("crm_esit.LLC_BI_Policy_Exception_c")

# COMMAND ----------

# MAGIC %md #### display records

# COMMAND ----------

spark.sql(f"select * from crm_esit.LLC_BI_Policy_Exception_c limit 5").display()

# COMMAND ----------

# MAGIC %md ### (15) LLC_BI_Product_Package_Team_c File

# COMMAND ----------

# MAGIC %md #### adido-in file

# COMMAND ----------

df15=adido_in('ADIDO-476-LT_LLC_BI_Product_Package_Team_c',',')

# COMMAND ----------

# MAGIC %md #### save into table

# COMMAND ----------

df15.write.format("delta").mode("overwrite").saveAsTable("crm_esit.LLC_BI_Product_Package_Team_c")

# COMMAND ----------

# MAGIC %md #### read table

# COMMAND ----------

spark.sql(f"select * from crm_esit.LLC_BI_Product_Package_Team_c limit 5").display()

# COMMAND ----------

# MAGIC %md ### (16) LLC_BI_Product_Package_c File

# COMMAND ----------

# MAGIC %md #### adido-in

# COMMAND ----------

df16=adido_in('ADIDO-476-LT_LLC_BI_Product_Package_c',',')

# COMMAND ----------

# MAGIC %md #### save into table

# COMMAND ----------

df16.write.format("delta").mode("overwrite").saveAsTable("crm_esit.LLC_BI_Product_Package_c")

# COMMAND ----------

# MAGIC %md #### read table

# COMMAND ----------

spark.sql(f"select * from crm_esit.LLC_BI_Product_Package_c limit 10").display()

# COMMAND ----------

# MAGIC %md ### (17) LT_LLC_BI_Role_c File

# COMMAND ----------

df17=adido_in('ADIDO-476-LT_LLC_BI_Role_c',',')

# COMMAND ----------

# MAGIC %md #### save into table

# COMMAND ----------

df17.write.format("delta").mode("overwrite").saveAsTable("crm_esit.LT_LLC_BI_Role_c")

# COMMAND ----------

# MAGIC %md #### read table

# COMMAND ----------

spark.sql(f"select * from crm_esit.LT_LLC_BI_Role_c limit 5").display()

# COMMAND ----------

# MAGIC %md ### (18) User File

# COMMAND ----------

# MAGIC %md #### adido-in

# COMMAND ----------

df18=adido_in('ADIDO-476-LT_User',',')

# COMMAND ----------

# MAGIC %md #### save into table

# COMMAND ----------

df18.write.format("delta").mode("overwrite").saveAsTable("crm_esit.User")

# COMMAND ----------

# MAGIC %md #### read table

# COMMAND ----------

spark.sql(f"select * from crm_esit.User limit 5").display()

# COMMAND ----------

# MAGIC %md ### (19) facility_type_mapping File

# COMMAND ----------

# MAGIC %md #### adido-in file

# COMMAND ----------

df19=adido_in('ADIDO-476-LT_facility_type_mapping',',')

# COMMAND ----------

# MAGIC %md #### save into table

# COMMAND ----------

df19.write.format("delta").mode("overwrite").saveAsTable("crm_esit.facility_type_mapping")

# COMMAND ----------

# MAGIC %md #### read table

# COMMAND ----------

spark.sql(f"select * from crm_esit.facility_type_mapping limit 10").display()

# COMMAND ----------

# MAGIC %md ## THE END

# COMMAND ----------



# COMMAND ----------

# MAGIC %md ##CHECK ADIDO-IN DATA EXISTS

# COMMAND ----------

# MAGIC %fs ls abfss://ca00bbacsm0001@edaaaedle1pradido.dfs.core.windows.net/data-in/CA00BBACSM0001/thural2

# COMMAND ----------

czJdbcUrl  = "jdbc:sqlserver://p3001-eastus2-asql-2.database.windows.net:1433;database=eda-akora2-aaecz-corporatepoolprd;loginTimeout=10;"    
srzJdbcUrl = "jdbc:sqlserver://p3001-eastus2-asql-3.database.windows.net:1433;database=eda-akora2-aaedl-srzpoolprd;loginTimeout=10;"
azJdbcUrl  = "jdbc:sqlserver://p3002-eastus2-asql-192.database.windows.net:1433;database=eda-akora-aaaz-ca00bbacsm0001poolprd;loginTimeout=10;"

# Get jdbc credentials directly from ADB scope
jdbcUsername = dbutils.secrets.get(scope = "aaaz-base", key = "SP_ADB_AAAZ_CA00BBACSM0001_PRD_AppID")
jdbcPassword = dbutils.secrets.get(scope = "aaaz-base", key = "SP_ADB_AAAZ_CA00BBACSM0001_PRD_PWD")

## aaaz-base/SP_ADB_AAAZ_CA00BBACSM0001_PRD_AppID
connectionProperties = {
  "AADSecurePrincipalId" : jdbcUsername,
  "AADSecurePrincipalSecret" : jdbcPassword,
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver",
  "authentication": "ActiveDirectoryServicePrincipal" ,
  "fetchsize":"10"
}