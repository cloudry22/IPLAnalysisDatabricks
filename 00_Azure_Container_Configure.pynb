# Databricks notebook source
# MAGIC %md
# MAGIC Go to 
# MAGIC "workspaceURL"#secrets/createScope

# COMMAND ----------


dbutils.fs.mount(
    source="wasbs://ipldata@demosstorage007.blob.core.windows.net",
    mount_point="/mnt/IPLData",
    extra_configs={
    "fs.azure.sas.ipldata.demosstorage007.blob.core.windows.net":dbutils.secrets.get(scope="GetIPLContainer", key="IPLContainerAccess")
  }

)


# COMMAND ----------

dbutils.fs.ls("/mnt/IPLData")
