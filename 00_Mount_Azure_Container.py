# Databricks notebook source
# MAGIC %fs
# MAGIC help mount

# COMMAND ----------

dbutils.fs.mount(
source="wasbs://ipl@nsestockdatastorage.blob.core.windows.net",
mount_point="/mnt/ipl_data",
extra_configs={'fs.azure.account.key.nsestockdatastorage.blob.core.windows.net':'8rS2/hkGnoqr8IJgJQJW1CHHyJq2nkjzfJYoByOTFi/xRb79F5nQCR00sPbtYfPi4UVrq6KIkNZT+AStPtuogw=='}
    
)


# COMMAND ----------

dbutils.fs.ls("/mnt/ipl_data")

# COMMAND ----------

# MAGIC %md
# MAGIC Go to 
# MAGIC "workspaceURL"#secrets/createScope

# COMMAND ----------

IPLContainerKey=dbutils.secrets.get('GetIPLContainer','IPLContainerKey')
IPLContainerValue=dbutils.secrets.get('GetIPLContainer','IPLContainerValue')

# COMMAND ----------

dbutils.fs.unmount("/mnt/ipl_data")


# COMMAND ----------

dbutils.fs.mount(
source="wasbs://ipl@nsestockdatastorage.blob.core.windows.net",
mount_point="/mnt/ipl_data",
extra_configs={IPLContainerKey:IPLContainerValue}
    
)

