# Databricks notebook source
dbutils.secrets.help()

# COMMAND ----------

# list all Secret Scopes
dbutils.secrets.listScopes()

# COMMAND ----------

# lists the secret keys
dbutils.secrets.list("formula1-scope")

# COMMAND ----------

dbutils.secrets.get(scope="formula1-scope", key="formula1dl-accountkey")

# COMMAND ----------


