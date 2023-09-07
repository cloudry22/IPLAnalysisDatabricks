-- Databricks notebook source
 use  IPL

-- COMMAND ----------

select season ,count(*) from IPL_MATCHES_CLEANED
group by season
order by 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #All Time Batting Leaders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Most Runs

-- COMMAND ----------

select season,batter,sum(batsman_run)  as total_runs from IPL_DETAILS
group by season,batter
order by 3 desc
limit 15

-- COMMAND ----------

-- MAGIC
-- MAGIC
-- MAGIC %md
-- MAGIC #### Most Fours

-- COMMAND ----------

select batter,count(*) as No_Of_Fours from IPL_DETAILS where batsman_run=4
group by batter
order by 2 desc
limit 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Most Six

-- COMMAND ----------

select batter,count(*) as No_Of_Six from IPL_DETAILS where batsman_run=6
group by batter
order by 2 desc
limit 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Highest Score

-- COMMAND ----------

select
  m.Season,
  m.MatchNumber,
  case
    when array_contains(Team1Players, batter) Then Team1
    when array_contains(Team2Players, batter) then Team2
    else null
  end as From_Team,case
    when array_contains(Team1Players, batter) Then Team2
    when array_contains(Team2Players, batter) then Team1
    else null
  end as Opposition_Team,
  batter,
  Sum(batsman_run) as Highest_Score
from
  IPL_DETAILS m
group by
  m.Season,
  m.MatchNumber,
  batter,
  From_Team,
  Opposition_Team
Order by
  6 desc
  limit 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Highest StrikeRate minimum 100 balls faced

-- COMMAND ----------

select batter,count(distinct id) as NoOfMatches,cast((sum(batsman_run)/count(batsman_run)*100) as decimal(10,2)) as StrikeRate from   IPL_DETAILS
group by batter
having count(batsman_run)>=100
order by 3 desc
limit 20

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Highest Average minimum 15 Matches

-- COMMAND ----------

select batter as Batsman,cast(sum(batsman_run)/sum(isWicketDelivery)  as decimal(10,2)) as Average
from IPL_DETAILS  
group by batter 
having count(distinct id)>=15
order by 2 desc
limit 15


-- COMMAND ----------

Create or Replace Temp View ScorePerBall
as
select id,Season , MatchNumber,Innings,overs,ballnumber,batter,sum(batsman_run) over(partition by Season , MatchNumber,Innings,batter order by Innings,overs,ballnumber rows between unbounded 
preceding and current row) as runs_scored ,
Row_Number() over(partition by Season , MatchNumber,Innings,batter order by Innings,overs,ballnumber rows between unbounded 
preceding and current row) as balls_faced
from IPL_DETAILS
where extra_type='NA'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Fastest 50 & 100

-- COMMAND ----------

select * from (
select Season , MatchNumber,Innings,overs,ballnumber,batter,balls_faced,min(runs_Scored)
,row_number() over(partition by Season , MatchNumber,Innings,batter order by balls_faced) as FiftyCompleted
from ScorePerBall where 
runs_Scored>=50

group by Season , MatchNumber,Innings,overs,ballnumber,batter,balls_faced
)x where FiftyCompleted=1
order by balls_faced
limit  10

-- COMMAND ----------

select * from (
select Season , MatchNumber,Innings,overs,ballnumber,batter,balls_faced,min(runs_Scored)
,row_number() over(partition by Season , MatchNumber,Innings,batter order by balls_faced) as FiftyCompleted
from ScorePerBall where 
runs_Scored>=100

group by Season , MatchNumber,Innings,overs,ballnumber,batter,balls_faced
)x where FiftyCompleted=1
order by balls_faced
limit 10

-- COMMAND ----------

select batter,count(*) as total_fifty
from 
(
select season,MatchNumber,batter,max(runs_scored) as runs_scored from ScorePerBall 
where runs_scored>=50 and runs_scored<=99 
group by season,MatchNumber,batter
)x
group by batter
order by 2 desc
limit 10

-- COMMAND ----------

select batter,count(*) as total_hundred
from 
(
select season,MatchNumber,batter,max(runs_scored) as runs_scored from ScorePerBall 
where runs_scored>=100
group by season,MatchNumber,batter
)x
group by batter
order by 2 desc
limit 10
