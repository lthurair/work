# Databricks notebook source
# MAGIC %md ## Set up Connections

# COMMAND ----------

# MAGIC %run /Shared/Self_Migration_Jobs/Functions/Initialize_Connections

# COMMAND ----------

# MAGIC %run /Shared/Self_Migration_Jobs/Functions/Initialize_Functions

# COMMAND ----------

# MAGIC %md ### List all tables in data-in folder

# COMMAND ----------

show_adido_in_content()

# COMMAND ----------

dbutils.fs.ls("abfss://ca00bb0dga0001@edaaaedle1prdadido.dfs.core.windows.net/data-in/CA00BB0DGA0001/thural2")

# COMMAND ----------

# MAGIC %md ### Create Schema (Database)

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists bbart_test_cycle2;
# MAGIC --drop schema crm_esit
# MAGIC --show tables in crm_esit

# COMMAND ----------

# MAGIC %md ### SPECIFY FILES

# COMMAND ----------

file=['account','bbcrm_log__c','bbdrc_branch_hierarchy','borrowing_option__c','cif_account_team_rel','cif_business_customer','cif_customer_account_rel','llc_bi__loan__c','llc_bi__product_package__c','llc_bi__product_package_team__c','llc_bi__role__c','llc_bi__covenant2__c','llc_bi__covenant_type__c','llc_bi__legal_entities','llc_bi__fee__c','term_condition__c','term_condition_type__c','product2','opportunitylineItem','opportunity','llc_bi__collateral__c','llc_bi__account_collateral','llc_bi__loan_collateral2__c','llc_bi__connection__c','guarantee_in_support_of__c','llc_bi__account_covenant__c','llc_bi__accountdocument__c','llc_bi__collateral_type__c','llc_bi__covenant_compliance2__c','llc_bi__loan_covenant__c','llc_bi__policy_exception__c','llc_bi__policy_exception_template__c','llc_bi__product_feature__c','loan_term_condition__c','risk_rating_historical_data__c','sic_code_table__c','user','currencytype','llc_bi__connection_role__c','llc_bi__document_placeholder__c','llc_bi__llc_loandocument__c','llc_bi__excluded_exposure__c','lon_business_facility','lon_business_account','lon_loan_information']

# COMMAND ----------

for x in file:
    df = adido_in(f"ADIDO-4189-{x}", delimiter=',')
    df.write.format("delta").mode("overwrite").saveAsTable(f"bbart_test_cycle2.{x}")

# COMMAND ----------

spark.sql(f"select * from bbart_test_cycle2.account limit 10").display()