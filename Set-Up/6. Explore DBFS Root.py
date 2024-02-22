# Databricks notebook source
# MAGIC %md
# MAGIC 1. List all folders in DBFS root
# MAGIC 2. Interact with DBFS File Browser
# MAGIC 3. Upload File to DBFS Root

# COMMAND ----------

display(dbutils.fs.ls("/"))

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/"))

# COMMAND ----------

display(spark.read.csv("/FileStore/circuits.csv"))

# COMMAND ----------


