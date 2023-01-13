# Databricks notebook source
# MAGIC %fs
# MAGIC help mount

# COMMAND ----------

def file_exists(path):
  try:
    dbutils.fs.ls(path)
    dbutils.fs.unmount(path)
  except Exception as e:
    if 'java.io.FileNotFoundException' in str(e):
      return False
    else:
      raise

# COMMAND ----------

mount_point="/mnt/ipl_data"
file_exists(mount_point)    


# COMMAND ----------

IPLContainerKey=dbutils.secrets.get('GetIPLContainer','IPLContainerKey')
IPLContainerValue=dbutils.secrets.get('GetIPLContainer','IPLContainerValue')

# COMMAND ----------

# MAGIC %md
# MAGIC Go to 
# MAGIC "workspaceURL"#secrets/createScope

# COMMAND ----------

dbutils.fs.mount(
source="wasbs://iplraw@nsestockdatastorage.blob.core.windows.net",
mount_point="/mnt/ipl_data",
extra_configs={IPLContainerKey:IPLContainerValue}
    
)


# COMMAND ----------

dbutils.fs.ls("/mnt/ipl_data/data/")

# COMMAND ----------


