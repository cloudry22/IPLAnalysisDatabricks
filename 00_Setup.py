# Databricks notebook source
SourceLocation="/mnt/ipl_data/data/Source/"
TargetLocation="/mnt/ipl_data/data/Target/"
SchemaLocation="/mnt/ipl_data/Schema/"
CheckpointLocation="/mnt/ipl_data/CheckPoint/"
DownStreamOutput="/mnt/ipl_data/data/DownStream/"

# COMMAND ----------

def cleanup(directory):
    if directory:
       dbutils.fs.rm(directory,True)
    else:
        dbutils.fs.mkdirs(directory)

# COMMAND ----------



# COMMAND ----------

dbutils.fs.rm("/mnt/ipl_data/data/",True)

# COMMAND ----------

dbutils.fs.mkdirs(SourceLocation)

# COMMAND ----------

dbutils.fs.mkdirs(TargetLocation)

# COMMAND ----------

dbutils.fs.mkdirs(CheckpointLocation)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from json.`/mnt/ipl_data/data/Target/*.json`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from MatchPravin
