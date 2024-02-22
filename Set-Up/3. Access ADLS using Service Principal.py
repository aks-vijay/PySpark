# Databricks notebook source
# MAGIC %md
# MAGIC #### Access ADLS using Azure Service Principal
# MAGIC 1. Register Azure AD Application / Service Principal
# MAGIC 2. Generate Secret / Password for the application
# MAGIC 3. Set Spark Config for App / Client ID, Directory / Tenant ID & Secret
# MAGIC 4. Assign Role "Storage Blob Data Contributer" to the Data Lake

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



spark.conf.set("fs.azure.account.auth.type.azstoragedatabricksgen2.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.azstoragedatabricksgen2.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.azstoragedatabricksgen2.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.azstoragedatabricksgen2.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.azstoragedatabricksgen2.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------


display(dbutils.fs.ls("abfss://demo@azstoragedatabricksgen2.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@azstoragedatabricksgen2.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


