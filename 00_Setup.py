# Databricks notebook source
BasePath="/mnt/IPLData/"
SourceFiles=BasePath+"SourceFiles/"
TargetLocationMatches=BasePath+"TargetMatches/"
TargetLocationBallByBall=BasePath+"TargetBallByBall/"
SchemaLocation=BasePath+"Schema/"
CheckpointLocation=BasePath+"CheckPoint/"
StorageLocation=BasePath+"Storage/"



# COMMAND ----------

dbutils.fs.mkdirs(SourceFiles)
dbutils.fs.mkdirs(TargetLocationMatches)
dbutils.fs.mkdirs(TargetLocationBallByBall)
dbutils.fs.mkdirs(SchemaLocation)
dbutils.fs.mkdirs(CheckpointLocation)
dbutils.fs.mkdirs(StorageLocation)

# COMMAND ----------

# MAGIC %sql
# MAGIC Create database if not exists IPL

# COMMAND ----------


