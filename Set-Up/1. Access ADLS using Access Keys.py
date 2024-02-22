# Databricks notebook source
scope = "formula1-scope"
key = "formula1dl-accountkey"
formula1_account_key = dbutils.secrets.get(scope=scope, key=key)

# COMMAND ----------

# set spark config to authenticate with Azure Storage Container
spark.conf.set(
                "fs.azure.account.key.azstoragedatabricksgen2.dfs.core.windows.net",
               formula1_account_key
               )
# Access the container using ABFS Driver (Azure Blob File Storage Driver)
display(dbutils.fs.ls("abfss://demo@azstoragedatabricksgen2.dfs.core.windows.net"))


# COMMAND ----------


