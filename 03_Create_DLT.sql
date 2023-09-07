-- Databricks notebook source
create or replace streaming live  table IPL_BALL_DETAILS_RAW

comment "Bronze Table"
as
select * from cloud_files('${TargetLocationBallByBall}',"json",map("cloudFiles.inferColumnTypes","true",
"header", "true",
"quote", '"'
))

-- COMMAND ----------

create or replace streaming live  table IPL_MATCHES_RAW

comment "Bronze Table"
as
select * from cloud_files('${TargetLocationMatches}',"json",map("cloudFiles.inferColumnTypes","true",
"header", "true",
"quote", '"'
))

-- COMMAND ----------

create or replace  streaming live   table IPL_MATCHES_CLEANED
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
 stream( live.IPL_MATCHES_RAW)

-- COMMAND ----------

create or replace streaming  live  table IPL_BALL_DETAILS_CLEANED

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
  case when player_out !='NA' then 1 else 0 end as isWicketDelivery,
  player_out,
  kind,
  fielders_involved,
  BattingTeam
from
 stream( live.IPL_BALL_DETAILS_RAW)


-- COMMAND ----------

create or replace live  table IPL_Details

comment "Gold Table"
select

  B.ID,
  M.city,
  M.Match_Date,
  M.Season,
  M.MatchNumber,
  M.Team1,
  M.Team2,
  M.Venue,
  M.TossWinner,
  M.TossDecision,
  M.WinningTeam,
  M.WonBy,
  M.Margin,
  M.Player_of_Match,
  M.Team1Players,
  M.Team2Players,
  M.Umpire1,
  M.Umpire2,
  B.innings,
  B.overs,
  B.ballnumber,
  B.batter,
  B.bowler,
  B.Non_Striker,
  B.extra_type,
  B.batsman_run,
  B.extrarun,
  B.total_run,
  B.isWicketDelivery,
  B.player_out,
  B.kind,
  B.fielders_involved,
  B.BattingTeam
from
  live.IPL_BALL_DETAILS_CLEANED B inner join live.IPL_MATCHES_CLEANED M 
  on B.ID=M.ID


