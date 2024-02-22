# Databricks notebook source
scope = "formula1-scope"
key = "formula1-sastoken"
formula1_account_key = dbutils.secrets.get(scope=scope, key=key)

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.azstoragedatabricksgen2.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.azstoragedatabricksgen2.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.azstoragedatabricksgen2.dfs.core.windows.net", formula1_account_key)

# Access the container using ABFS Driver (Azure Blob File Storage Driver)
display(dbutils.fs.ls("abfss://demo@azstoragedatabricksgen2.dfs.core.windows.net"))

# COMMAND ----------


