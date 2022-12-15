-- Databricks notebook source
create or replace streaming live  table IPL_MATCHES_RAW
(constraint ID_Not_NULL expect(ID is not null) on violation drop row)
comment "Bronze Table"

as
select * from cloud_files("dbfs:/mnt/ipl_data/Matches/","csv",map("cloudFiles.inferColumnTypes","true",
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
  "non - striker" as Non_Striker,
  extra_type,
  batsman_run,
  extras_run,
  total_run,
  non_boundary,
  isWicketDelivery,
  player_out,
  kind,
  fielders_involved,
  BattingTeam
from
  stream(live.IPL_BALL_DETAILS_RAW)


-- COMMAND ----------

create or replace  live  table IPL_DETAILS

comment "Silver Table"
as

select 
M.ID,
M.City,
M.Match_Date,
M.Season,
M.MatchNumber,
M.Team1,
M.Team2,
M.Venue,
M.TossWinner,
M.TossDecision,
M.SuperOver,
M.WinningTeam,
M.WonBy,
M.Margin,
M.method,
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
B.extras_run,
B.total_run,
B.non_boundary,
B.isWicketDelivery,
B.player_out,
B.kind,
B.fielders_involved,
B.BattingTeam
from live.IPL_MATCHES_CLEANED M

inner join live.IPL_BALL_DETAILS_CLEANED B

on
M.ID=B.ID

-- COMMAND ----------

create or replace  live  table  ScorePerBall
comment "Gold Table"

as
select id,Season , MatchNumber,Innings,overs,ballnumber,batter,sum(batsman_run) over(partition by Season , MatchNumber,Innings,batter order by Innings,overs,ballnumber rows between unbounded 
preceding and current row) as runs_scored ,
Row_Number() over(partition by Season , MatchNumber,Innings,batter order by Innings,overs,ballnumber rows between unbounded 
preceding and current row) as balls_faced
from live.IPL_DETAILS
where extra_type not in ('wides') 
