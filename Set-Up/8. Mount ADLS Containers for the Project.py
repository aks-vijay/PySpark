# Databricks notebook source
# MAGIC %md
# MAGIC #### Access ADLS using Azure Service Principal
# MAGIC 1. Register Azure AD Application / Service Principal
# MAGIC 2. Generate Secret / Password for the application
# MAGIC 3. Set Spark Config for App / Client ID, Directory / Tenant ID & Secret
# MAGIC 4. Call the File System to Mount the Storage
# MAGIC 5. Assign Role "Storage Blob Data Contributer" to the Data Lake

# COMMAND ----------

def mount_adls(storage_acc_name, container_name):
    scope = "formula1-scope"
    key = "formula1-clientsecret"
    formula1_client_secret = dbutils.secrets.get(scope=scope, key=key)

    # Once you register the APP in Azure Entra ID App Registeration => Fetch from Overview
    client_id = "d9b32840-7bfc-403a-a9c3-b480336f0679"
    tenant_id = "b617d216-9964-4078-8e72-fb17378854c1"
    client_secret = formula1_client_secret

    # from microsoft document (https://learn.microsoft.com/en-us/azure/databricks/dbfs/mounts)
    configs = {"fs.azure.account.auth.type": "OAuth",
            "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
            "fs.azure.account.oauth2.client.id": client_id,
            "fs.azure.account.oauth2.client.secret": client_secret,
            "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    # mount the ADLS to DBFS to access the Notebooks
    # One benefit is we dont need to add the full path with ABFS because its mounted on the DBFS => Can directly access from the DBFS mount

    # unmount if mounts exists
    if any(mount.mountPoint == f"/mnt/{storage_acc_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_acc_name}/{container_name}")

    dbutils.fs.mount(
        source=f"abfss://{container_name}@{storage_acc_name}.dfs.core.windows.net/",
        mount_point=f"/mnt/{storage_acc_name}/{container_name}",
        extra_configs=configs
    )

    display(dbutils.fs.mounts())

# COMMAND ----------

mount_adls(storage_acc_name="azstoragedatabricksgen2", container_name="raw")
mount_adls(storage_acc_name="azstoragedatabricksgen2", container_name="processed")
mount_adls(storage_acc_name="azstoragedatabricksgen2", container_name="presentation")
