# Databricks notebook source
# MAGIC %run ./00_Setup

# COMMAND ----------

import json
import pyspark.sql.functions as F
from pyspark.sql.types import *

SchemaFile=dbutils.fs.head(SchemaLocation+"IPLMatches.txt")
new_schema = StructType.fromJson(json.loads(SchemaFile))

# COMMAND ----------

IPLDataset = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("multiline", "true")
    #.option("cloudFiles.inferColumnTypes", "true")
    #.option("cloudFiles.schemaLocation", SchemaLocation)
    .schema(new_schema)
    .load(SourceFiles)
    .select("*","_metadata.file_name")
)

# COMMAND ----------


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
 Innings + 1 as innings,
  OverNo as overs,
  BallNumber as ballnumber ,
  ball.batter ,
  ball.bowler,
  ball.non_striker,
  case when ball.extras.byes is not null then 'byes' 
       when ball.extras.legbyes is not null then 'legbyes'
       when ball.extras.noballs is not null then 'noballs'
       when ball.extras.penalty is not null then 'penalty'
       when ball.extras.wides is not null then 'wides'
   else null end as extra_type,
  ball.runs.batter as batsman_run,

ball.runs.extras as extrarun,
  ball.runs.total as total_run,
  ball.wickets.player_out [0] as player_out,
  ball.wickets.kind [0] as kind,

  ball.wickets.fielders.name [0] [0] as fielders_involved,
    BattingTeam

from
  BallDetails"""
).na.fill("NA")

# COMMAND ----------

MatchInfo = IPLDataset.selectExpr("info", "replace(file_name,'.json') as ID")

# COMMAND ----------

MatchInfo.createOrReplaceTempView("MatchInfo")

# COMMAND ----------

MatchDetails=spark.sql("""SELECT 
    ID
    ,info.city
	,info.dates[0] AS Date
   	,info.season as Season

	,coalesce(info.event.match_number,info.event.stage) as MatchNumber
    
    ,info.teams [0] AS Team1
	,info.teams [1] AS Team2
    ,info.venue as Venue
    
  	,info.toss.winner AS TossWinner
	,info.toss.decision AS TossDecision
    ,null as super_over
    ,info.outcome.winner AS WinningTeam
    ,case when info.outcome.by.runs is not null then 'Runs' when info.outcome.by.wickets is not null then 'Wickets' else 'SuperOver' end as WonBy
    ,coalesce(info.outcome.by.runs,info.outcome.by.wickets) as margin
	,null as method
    ,info.player_of_match [0] AS Player_of_Match


    ,filter(array(info.players.*), x -> x IS NOT NULL)[0] as Team1Players
    ,filter(array(info.players.*), x -> x IS NOT NULL)[1] as Team2Players
    ,info.officials.umpires [0] AS Umpire1
	,info.officials.umpires [1] AS Umpire2
	
FROM MatchInfo

""").na.fill("NA")


# COMMAND ----------

MatchDetails.writeStream    .format("json")    .trigger(availableNow=True)    .option("checkpointLocation", CheckpointLocation+"/MatchDetails")    .option("path", TargetLocationMatches)    .outputMode("append")    .table("MatchDetails")


# COMMAND ----------

BallByBall.writeStream    .format("json")    .trigger(availableNow=True)    .option("checkpointLocation", CheckpointLocation+"/BallByBall")    .option("path", TargetLocationBallByBall)    .outputMode("append")    .table("BallByBallDetails")


# COMMAND ----------


