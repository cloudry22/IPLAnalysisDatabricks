# Databricks notebook source
IPLMatchDetails = spark.read.format("json").option("multiline","true").option("inferSchema","true").load("/mnt/ipl_data/data/*.json")


# COMMAND ----------

from pyspark.sql.functions import split, explode ,col

innings=IPLMatchDetails.select("innings")

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import *

inningsDetail=innings.withColumn("new",F.arrays_zip("innings.team","innings.overs")).withColumn("new",F.explode("new")).selectExpr("new.team as BattingTeam","new.overs as overs")


# COMMAND ----------

inningsDetail.createOrReplaceTempView("inningsDetail")

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view OverDetails
# MAGIC as
# MAGIC select BattingTeam,explode(overs)as over from inningsDetail

# COMMAND ----------

# MAGIC %sql
# MAGIC Create or replace temp view BallDetails 
# MAGIC as
# MAGIC select BattingTeam,OverNo,pos+1 as BallNumber,ball from (
# MAGIC select BattingTeam,over.over as OverNo,posexplode(over.deliveries) as (pos,ball)   from OverDetails
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select BattingTeam,OverNo,BallNumber,ball.batter as batsman,ball.bowler,ball.non_striker,ball.wickets.fielders.name[0][0] as FieldingPlayer,ball.wickets.kind[0] as dissmisalType,ball.wickets.player_out[0] as player_out,ball.runs.batter as batsman_run,ball.extras.* ,ball.runs.extras as  extrarun,ball.runs.total as total_run from BallDetails

# COMMAND ----------

MatchInfo=IPLMatchDetails.select("info")

# COMMAND ----------

MatchInfo.display()

# COMMAND ----------

MatchInfo.createOrReplaceTempView("MatchInfo")

# COMMAND ----------

MatchInfo.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC select info.teams,filter(array(info.players.*), x -> x IS NOT NULL)[0] as Team1,filter(array(info.players.*), x -> x IS NOT NULL)[1] as Team2  from MatchInfo

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT info.city
# MAGIC 	,info.dates[0] AS match_day
# MAGIC 	,info.event.*
# MAGIC 	,info.officials.match_referees [0] AS match_referees
# MAGIC 	,info.officials.reserve_umpires [0] AS reserve_umpires
# MAGIC 	,info.officials.tv_umpires [0] AS tv_umpires
# MAGIC 	,info.officials.umpires [0] AS Umpire1
# MAGIC 	,info.officials.umpires [1] AS Umpire2
# MAGIC 	,info.outcome.*
# MAGIC 	,info.outcome.winner AS winningTeam
# MAGIC 	,info.player_of_match [0] AS player_of_match
# MAGIC 	,info.season
# MAGIC 	,info.teams [0] AS team1
# MAGIC 	,info.teams [1] AS team2
# MAGIC 	,info.toss.decision AS toss_decision
# MAGIC 	,info.toss.winner AS toss_winner
# MAGIC FROM MatchInfo
# MAGIC where info.dates[0]='2009-04-23'
# MAGIC order by match_number
