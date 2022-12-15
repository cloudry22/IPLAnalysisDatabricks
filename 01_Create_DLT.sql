-- Databricks notebook source
create or replace streaming live  table IPL_MATCHES_RAW

comment "Bronze Table"
as
select * from cloud_files("dbfs:/mnt/ipl_data/Matches/","csv",map("cloudFiles.inferColumnTypes","true",
"header", "true",
"quote", '"'
))

-- COMMAND ----------

create or replace streaming live  table IPL_MATCHES_CLEANED
comment "Silver Table"
as
select
  ID,
  City,
  cast(Date as date) as Match_Date,
  Season,
  MatchNumber,
  Team1,
  Team2,
  Venue,
  TossWinner,
  TossDecision,
  SuperOver,
  WinningTeam,
  WonBy,
  Margin,
  method,
  Player_of_Match,
  split(regexp_replace(Team1Players,"\\'|\\[|\\]",''),", ") as Team1Players,
  split(regexp_replace(Team2Players,"\\'|\\[|\\]",''),", ") as Team2Players,
  Umpire1,
  Umpire2
from
  stream(live.IPL_MATCHES_RAW)

-- COMMAND ----------

create or replace streaming live  table IPL_BALL_DETAILS_RAW

comment "Bronze Table"
as
select * from cloud_files("dbfs:/mnt/ipl_data/BallByBall/","csv",map("cloudFiles.inferColumnTypes","true",
"header", "true",
"quote", '"'
))
