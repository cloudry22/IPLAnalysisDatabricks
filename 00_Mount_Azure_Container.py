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

import json
df1 = spark.read\
            .format("json")\
            .option("multiline", "true")\
            .option("inferSchema","true")\
            .load('/mnt/ipl_data/data/Source/')
            
schema_json=df1.schema.json()


# COMMAND ----------

schema_json

# COMMAND ----------

dbutils.fs.rm("/mnt/ipl_data/data/Schema/IPLMatches.txt")

dbutils.fs.put("/mnt/ipl_data/data/Schema/IPLMatches.txt", schema_json)





# COMMAND ----------

dbutils.fs.head("/mnt/ipl_data/data/Schema/IPLMatches.txt")


# COMMAND ----------

OutputFile="/mnt/ipl_data/data/Schema/SchemaFile.txt"
with open(OutputFile, "w") as f:
  # Write the variable output to the file
  f.write(schema_json)

# COMMAND ----------

dbutils.fs.help() 
