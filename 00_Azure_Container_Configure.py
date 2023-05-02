# Databricks notebook source
IPLSharedAccessKey=dbutils.secrets.get('GetIPLContainer','IPLSasToken')

# COMMAND ----------

# MAGIC %md
# MAGIC Go to 
# MAGIC "workspaceURL"#secrets/createScope

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.iplstoragepravin.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.iplstoragepravin.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.iplstoragepravin.dfs.core.windows.net", IPLSharedAccessKey)

# COMMAND ----------

dbutils.fs.ls("abfss://ipldata@iplstoragepravin.dfs.core.windows.net/")

