-- Databricks notebook source
use IPL

-- COMMAND ----------

create table if not exists FantasyPoints
(
Type string,
score string,
points smallint
);

-- COMMAND ----------



delete from FantasyPoints;
INSERT INTO FantasyPoints(Type,Score,Points) values('Batting',1,1);
INSERT INTO FantasyPoints(Type,Score,Points) values('Batting',4,1);
INSERT INTO FantasyPoints(Type,Score,Points) values('Batting',6,2);
INSERT INTO FantasyPoints(Type,Score,Points) values('Batting',50,8);
INSERT INTO FantasyPoints(Type,Score,Points) values('Batting',100,8);
INSERT INTO FantasyPoints(Type,Score,Points) values('Batting',0,-2);
INSERT INTO FantasyPoints(Type,Score,Points) values('Wicket',1,25);
INSERT INTO FantasyPoints(Type,Score,Points) values('Wicket',4,8);
INSERT INTO FantasyPoints(Type,Score,Points) values('Wicket',5,16);
INSERT INTO FantasyPoints(Type,Score,Points) values('Maiden','NA',8);
INSERT INTO FantasyPoints(Type,Score,Points) values('Lineup','NA',4);
INSERT INTO FantasyPoints(Type,Score,Points) values('Catch','NA',8);
INSERT INTO FantasyPoints(Type,Score,Points) values('Direct Runout','NA',12);


-- COMMAND ----------

select * from FantasyPoints

-- COMMAND ----------

select ID,batter,sum(batsman_run)  as total_runs from IPL_DETAILS
group by ID,batter
order by 3 desc
limit 15

-- COMMAND ----------

create or replace temporary view  runs_scored_with_points
as

with bastman_run_points
as(
select  ID ,batter ,case when batsman_run=1 then 0 else batsman_run end+f.points as Points from IPL_DETAILS i,FantasyPoints f
where
i.batsman_run=f.score
and f.type='Batting'
and f.score in (1,4,6)
)
select * from bastman_run_points

-- COMMAND ----------

merge into DreamScore l
USING
(
select ID,batter,sum(points) as sum_points from runs_scored_with_points 
group by ID,batter
)x
ON
l.ID=x.ID
and l.Player_List=x.batter
when matched then 
update set Dream_Points=l.Dream_Points+sum_points


-- COMMAND ----------

Create or Replace Temp View ScorePerBall
as
select id,Season , MatchNumber,Innings,overs,ballnumber,batter,sum(batsman_run) over(partition by Season , MatchNumber,Innings,batter order by Innings,overs,ballnumber rows between unbounded 
preceding and current row) as runs_scored ,
Row_Number() over(partition by Season , MatchNumber,Innings,batter order by Innings,overs,ballnumber rows between unbounded 
preceding and current row) as balls_faced
from IPL_DETAILS
where extra_type not in ('wides') 

-- COMMAND ----------

Create or replace table FiftyCompleted
as
select id,batter from (
select id,Season , MatchNumber,Innings,batter,balls_faced,min(runs_Scored)
,row_number() over(partition by Season , MatchNumber,Innings,batter order by balls_faced) as FiftyCompleted
from ScorePerBall where 
runs_Scored>=50

group by id,Season , MatchNumber,Innings,batter,balls_faced
)x
 where x.FiftyCompleted=1
order by balls_faced

-- COMMAND ----------

Create or replace table HundredCompleted
as
select id,batter  from (
select id,Season , MatchNumber,Innings,batter,balls_faced,min(runs_Scored) as min_runs_Scored
,row_number() over(partition by Season , MatchNumber,Innings,batter order by balls_faced) as FiftyCompleted
from ScorePerBall where 
runs_Scored>=100

group by id,Season , MatchNumber,Innings,batter,balls_faced
)x where FiftyCompleted=1
order by balls_faced

-- COMMAND ----------

merge into DreamScore l
USING
(
select * from FiftyCompleted f,FantasyPoints f1
where f1.type='Batting' and f1.score=50
)x
ON
l.ID=x.ID
and l.Player_List=x.batter
when matched then 
update set Dream_Points=l.Dream_Points+points


-- COMMAND ----------

merge into DreamScore l
USING
(
select * from HundredCompleted f,FantasyPoints f1
where f1.type='Batting' and f1.score=100
)x
ON
l.ID=x.ID
and l.Player_List=x.batter
when matched then 
update set Dream_Points=l.Dream_Points+points


-- COMMAND ----------

Create or replace view OutOnZero as 
with Runs_Scored 
as(
select ID,case when player_out='NA' then batter else  player_out end as batter ,sum(batsman_run) as TOTAL_RUNS from IPL_DETAILS
where  innings in (1,2)
group by ID,case when player_out='NA' then batter else  player_out end
),
is_out
(
select ID,case when player_out='NA' then batter else  player_out end as batter from IPL_DETAILS where isWicketDelivery=1  and innings in (1,2)
)
select r.ID,r.batter from Runs_Scored r inner join is_out o 
ON
r.ID=o.ID
AND
r.batter=o.batter
WHERE TOTAL_RUNS=0

-- COMMAND ----------

merge into DreamScore l
USING
(
select * from OutOnZero,FantasyPoints f1
where f1.type='Batting' and f1.score=0
)x
ON
l.ID=x.ID
and l.Player_List=x.batter
when matched then 
update set Dream_Points=l.Dream_Points+points

-- COMMAND ----------

merge into DreamScore l
USING
(
with temp1 as
(
select ID,bowler,sum(isWicketDelivery) as total_wickets from IPL_DETAILS 
where isWicketDelivery=1  and kind not in  ('run out')
group by ID,bowler
)

select ID,bowler,total_wickets,total_wickets*25+case when total_wickets=4 then 8 when total_wickets>=5 then 16 else 0 end as DreamPoints from temp1
)x
on
x.ID=l.ID
and l.Player_List=x.bowler
when matched then 
update set Dream_Points=l.Dream_Points+DreamPoints

-- COMMAND ----------

Create or replace table average_points
as
select Player_List,avg(Dream_Points) as average_points_Season,count(Player_List)as NoOfMarches from DreamScore
group by 1


-- COMMAND ----------

select * from average_points
where Player_List in (Select Player_List from DreamScore where Team in ('Gujarat Titans','Mumbai Indians') and season =2023)
order by 2 desc

