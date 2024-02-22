# Databricks notebook source
# MAGIC %md
# MAGIC #### Access ADLS using Azure Service Principal
# MAGIC 1. Register Azure AD Application / Service Principal
# MAGIC 2. Generate Secret / Password for the application
# MAGIC 3. Set Spark Config for App / Client ID, Directory / Tenant ID & Secret
# MAGIC 4. Call the File System to Mount the Storage
# MAGIC 5. Assign Role "Storage Blob Data Contributer" to the Data Lake

# COMMAND ----------

scope = "formula1-scope"
key = "formula1-clientsecret"
formula1_client_secret = dbutils.secrets.get(scope=scope, key=key)

# COMMAND ----------

# Once you register the APP in Azure Entra ID App Registeration => Fetch from Overview
client_id = "d9b32840-7bfc-403a-a9c3-b480336f0679"
tenant_id = "b617d216-9964-4078-8e72-fb17378854c1"
client_secret = formula1_client_secret

# COMMAND ----------

# from microsoft document (https://learn.microsoft.com/en-us/azure/databricks/dbfs/mounts)

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

# mount the ADLS to DBFS to access the Notebooks
# One benefit is we dont need to add the full path with ABFS because its mounted on the DBFS => Can directly access from the DBFS mount

dbutils.fs.mount(
    source="abfss://demo@azstoragedatabricksgen2.dfs.core.windows.net/",
    mount_point="/mnt/formula1dl/demo",
    extra_configs=configs
)

# COMMAND ----------

# Role needs to be assigned to the App in the Storage Account
# Access the container using ABFS Driver (Azure Blob File Storage Driver)
display(dbutils.fs.ls("/mnt/formula1dl/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/formula1dl/demo/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.unmount()
