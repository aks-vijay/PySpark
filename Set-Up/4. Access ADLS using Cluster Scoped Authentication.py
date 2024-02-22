# Databricks notebook source
# Access the container using ABFS Driver (Azure Blob File Storage Driver)
display(dbutils.fs.ls("abfss://demo@azstoragedatabricksgen2.dfs.core.windows.net"))

# COMMAND ----------

# MAGIC %fs ls 

# COMMAND ----------


