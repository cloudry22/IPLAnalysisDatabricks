-- Databricks notebook source
create or replace streaming live  table IPL_MATCHES_RAW
(constraint ID_Not_NULL expect(ID is not null) on violation drop row)
comment "Bronze Table"
as
select * from cloud_files("/mnt/ipl_data/data/TargetMatches/","json",map("cloudFiles.inferColumnTypes","true",
"header", "true",
"quote", '"'
))

-- COMMAND ----------

create or replace streaming live  table IPL_BALL_DETAILS_RAW

comment "Bronze Table"
as
select * from cloud_files("/mnt/ipl_data/data/TargetBallByBall/","json",map("cloudFiles.inferColumnTypes","true",
"header", "true",
"quote", '"'
))

-- COMMAND ----------

create or replace streaming live  table IPL_MATCHES_CLEANED
(constraint ID_Not_NULL expect(ID is not null) on violation drop row)
comment "Silver Table"
as
select
  ID,
  city,
  cast(Date as date) as Match_Date,
  Season,
  MatchNumber,
  Team1,
  Team2,
  Venue,
  TossWinner,
  TossDecision,
  WinningTeam,
  WonBy,
  Margin,
  Player_of_Match,
  Team1Players,
  Team2Players,
  Umpire1,
  Umpire2
from
  stream(live.IPL_MATCHES_RAW)

-- COMMAND ----------

create or replace streaming live  table IPL_BALL_DETAILS_CLEANED

comment "Silver Table"
select
  ID,
  innings,
  overs,
  ballnumber,
  batter,
  bowler,
  Non_Striker,
  extra_type,
  batsman_run,
  extrarun,
  total_run,
  case when player_out is not null then 1 else 0 end as isWicketDelivery,
  player_out,
  kind,
  fielders_involved,
  BattingTeam
from
  stream(live.IPL_BALL_DETAILS_RAW)


-- COMMAND ----------

select * from IPL.IPL_MATCHES_RAW
