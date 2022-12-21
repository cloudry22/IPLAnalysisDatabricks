# Databricks notebook source
# MAGIC %run /Repos/raut1606@gmail.com/IPLAnalysisDatabricks/00_Setup

# COMMAND ----------

import json
SchemaLocation=dbutils.fs.head("/mnt/ipl_data/data/Schema/IPLMatches.txt")
new_schema = StructType.fromJson(json.loads(SchemaLocation))

# COMMAND ----------



# COMMAND ----------

IPLDataset = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("multiline", "true")
    .option("cloudFiles.inferColumnTypes", "true")
    .schema(new_schema)
    .load(SourceLocation)
    .select("*","_metadata.file_name")
)

# COMMAND ----------

IPLDataset.display()

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import *

innings = IPLDataset.selectExpr("innings", "replace(file_name,'.json') as ID")


# COMMAND ----------

inningsDetail = (
    innings.withColumn("new", F.arrays_zip("innings.team", "innings.overs"))
    .selectExpr("ID", "posexplode(new) as (Innings,c)")
    .selectExpr("ID", "Innings", "c.team as BattingTeam", "c.overs")
)
inningsDetail.createOrReplaceTempView("inningsDetail")

# COMMAND ----------

# MAGIC %sql create
# MAGIC or replace temp view OverDetails as
# MAGIC select
# MAGIC   ID,
# MAGIC   Innings,
# MAGIC   BattingTeam,
# MAGIC   explode(overs) as over
# MAGIC from
# MAGIC   inningsDetail

# COMMAND ----------

# MAGIC %sql Create
# MAGIC or replace temp view BallDetails as
# MAGIC select
# MAGIC   ID,
# MAGIC   Innings,
# MAGIC   BattingTeam,
# MAGIC   OverNo,
# MAGIC   pos + 1 as BallNumber,
# MAGIC   ball
# MAGIC from
# MAGIC   (
# MAGIC     select
# MAGIC       ID,
# MAGIC       Innings,
# MAGIC       BattingTeam,
# MAGIC       over.over as OverNo,
# MAGIC       posexplode(over.deliveries) as (pos, ball)
# MAGIC     from
# MAGIC       OverDetails
# MAGIC   )

# COMMAND ----------

BallByBall = spark.sql(
    """select
  ID,
  BattingTeam,
  Innings + 1 as Innings,
  OverNo,
  BallNumber,
  ball.batter as batsman,
  ball.bowler,
  ball.non_striker,
  ball.wickets.fielders.name [0] [0] as FieldingPlayer,
  ball.wickets.kind [0] as dissmisalType,
  ball.wickets.player_out [0] as player_out,
  ball.runs.batter as batsman_run,
  ball.extras.*,
  ball.runs.extras as extrarun,
  ball.runs.total as total_run
from
  BallDetails"""
)

# COMMAND ----------

MatchInfo = IPLDataset.selectExpr("info", "replace(file_name,'.json') as ID")

# COMMAND ----------

MatchInfo.createOrReplaceTempView("MatchInfo")

# COMMAND ----------

# MAGIC %sql
# MAGIC select ID,info,info.outcome.by.* from MatchInfo where id=1312198501269

# COMMAND ----------

MatchDetails=spark.sql(""" 
SELECT 
    ID
    ,info.city
	,info.dates[0] AS match_day
	,info.event.match_number
	,info.officials.match_referees [0] AS match_referees
	,info.officials.reserve_umpires [0] AS reserve_umpires
	,info.officials.tv_umpires [0] AS tv_umpires
	,info.officials.umpires [0] AS Umpire1
	,info.officials.umpires [1] AS Umpire2
	,info.outcome.by.*
	,info.outcome.winner AS winningTeam
	,info.player_of_match [0] AS player_of_match
	,info.season
	,info.teams [0] AS team1
	,info.teams [1] AS team2
	,info.toss.decision AS toss_decision
	,info.toss.winner AS toss_winner
    ,filter(array(info.players.*), x -> x IS NOT NULL)[0] as FullTeam1
    ,filter(array(info.players.*), x -> x IS NOT NULL)[1] as FullTeam2
FROM MatchInfo

""")

# COMMAND ----------


MatchDetails.writeStream    .format("json")    .trigger(processingTime="10 seconds")    .option("checkpointLocation", CheckpointLocation)    .option("path", TargetLocation)    .outputMode("append")    .table("MatchPravin")
    

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from   MatchPravin where ID=4191641312200
