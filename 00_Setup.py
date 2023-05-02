# Databricks notebook source
# MAGIC %run ./00_Azure_Container_Configure

# COMMAND ----------

BasePath="abfss://ipldata@iplstoragepravin.dfs.core.windows.net/"
SourceFiles=BasePath+"SourceFiles/"
TargetLocationMatches=BasePath+"TargetMatches/"
TargetLocationBallByBall=BasePath+"TargetBallByBall/"
SchemaLocation=BasePath+"/Schema/"
CheckpointLocation=BasePath+"/CheckPoint/"
StorageLocation=BasePath+"/Storage/"



# COMMAND ----------

dbutils.fs.mkdirs(SourceFiles)
dbutils.fs.mkdirs(TargetLocationMatches)
dbutils.fs.mkdirs(TargetLocationBallByBall)
dbutils.fs.mkdirs(SchemaLocation)
dbutils.fs.mkdirs(CheckpointLocation)
dbutils.fs.mkdirs(StorageLocation)

# COMMAND ----------

spark.conf.set('Files.SourceFiles'    , SourceFiles)
spark.conf.set('Files.TargetLocationMatches'    , TargetLocationMatches)
spark.conf.set('Files.TargetLocationBallByBall'    , TargetLocationBallByBall)


# COMMAND ----------

# MAGIC %sql
# MAGIC Create Database if not exists IPL ;

# COMMAND ----------

# MAGIC %sql
# MAGIC use IPL;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table IF NOT EXISTS path_config
# MAGIC (
# MAGIC   id INTEGER,
# MAGIC   Name String,
# MAGIC   path string
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from path_config;
# MAGIC insert into path_config values(1,'SourceLocation','${SourceFiles.Files}');
# MAGIC --insert into path_config values(2,'TargetLocationMatches','${TargetLocationMatches}');
# MAGIC --insert into path_config values(3,'TargetLocationBallByBall',TargetLocationBallByBall);
# MAGIC select * from path_config;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from path_config
