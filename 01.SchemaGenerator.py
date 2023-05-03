# Databricks notebook source
# MAGIC %run ./00_Setup

# COMMAND ----------

import json
df1 = spark.read\
            .format("json")\
            .option("multiline", "true")\
            .option("inferSchema","true")\
            .load(SourceFiles)
            
schema_json=df1.schema.json()


# COMMAND ----------

schema_json

# COMMAND ----------

dbutils.fs.rm(SchemaLocation+"IPLMatches.txt")

dbutils.fs.put(SchemaLocation+"IPLMatches.txt", schema_json)




