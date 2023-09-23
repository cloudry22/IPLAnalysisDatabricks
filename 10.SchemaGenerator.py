# Databricks notebook source
# MAGIC %run /Repos/helmi.asari22@outlook.com/IPLAnalysisDatabricks/00_Setup

# COMMAND ----------

import json
df1 = spark.read\
            .format("json")\
            .option("multiline", "true")\
            .option("inferSchema","true")\
            .load(SourceCompleteLocation)
            
schema_json=df1.schema.json()


# COMMAND ----------

schema_json

# COMMAND ----------

dbutils.fs.rm("/mnt/ipl_data/data/Schema/IPLMatches.txt")

dbutils.fs.put("/mnt/ipl_data/data/Schema/IPLMatches.txt", schema_json)




