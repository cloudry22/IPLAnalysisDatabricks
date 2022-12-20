# Databricks notebook source
dbutils.fs.mkdirs("/mnt/ipl_data/data/")

# COMMAND ----------

from  pyspark.sql.functions import input_file_name


# COMMAND ----------

IPLDataset = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("multiline", "true")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaLocation", "/mnt/ipl_data/SchemaDataNew/")
    .load("/mnt/ipl_data/data/")
    .select("*","_metadata.file_name")
)

# COMMAND ----------

IPLDataset = (
    spark.read.format("json")
    .option("multiline", "true")
    .option("inferSchema", "true")
    .load("/mnt/ipl_data/data/")
        .select("*","_metadata.file_name")

)

# COMMAND ----------

IPLDataset.display()

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import *

innings=IPLDataset.selectExpr("innings" ,"replace(file_name,'.json') as ID")
info=IPLDataset.selectExpr("info","replace(file_name,'.json') as ID")

# COMMAND ----------


innings.createOrReplaceTempView("inningsDetail")

# COMMAND ----------

# MAGIC %sql
# MAGIC select arrays_zip("array(innings.team","innings.overs") from inningsDetail

# COMMAND ----------

inningsDetail=innings.withColumn("new",F.arrays_zip("innings.team","innings.overs")).withColumn("new",F.posexplode("new") as Seq("pos", "val")).selectExpr("ID","c.team as BattingTeam","c.overs as overs","p")
inningsDetail.createOrReplaceTempView("inningsDetail")

# COMMAND ----------

inningsDetail=innings.withColumn("new",F.arrays_zip("innings.team","innings.overs"))
inningsDetail.selectExpr("posexplode(s) as (p,c)")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from inningsDetail where ID=392190

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view OverDetails
# MAGIC as
# MAGIC select ID,BattingTeam,explode(overs)as over from inningsDetail

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from OverDetails

# COMMAND ----------

# MAGIC %sql
# MAGIC Create or replace temp view BallDetails 
# MAGIC as
# MAGIC select ID,BattingTeam,OverNo,pos+1 as BallNumber,ball from (
# MAGIC select ID,BattingTeam,over.over as OverNo,posexplode(over.deliveries) as (pos,ball)   from OverDetails
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select ID,BattingTeam,OverNo,BallNumber,ball.batter as batsman,ball.bowler,ball.non_striker,ball.wickets.fielders.name[0][0] as FieldingPlayer,ball.wickets.kind[0] as dissmisalType,ball.wickets.player_out[0] as player_out,ball.runs.batter as batsman_run,ball.extras.* ,ball.runs.extras as  extrarun,ball.runs.total as total_run from BallDetails
# MAGIC where ID=392190
