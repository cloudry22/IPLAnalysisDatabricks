-- Databricks notebook source
use IPL

-- COMMAND ----------

Create or replace temp view IPL_Matches_Details
as
select * from
IPLMatchDetailsTbl m
inner join 
IPLBallsDetailsTbl b
on
m.ID=b.MATCH_ID

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Most Wickets

-- COMMAND ----------

select bowler,count(*) as NoOfWickets from IPLBallsDetailsTbl
where isWicketDelivery=1
and kind not in ('run out','retired hurt','retired out')
group by bowler
order by 2 desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Most Maiden

-- COMMAND ----------

select bowler,count(*) as Maiden_Bowled
from
(
select distinct Match_id,innings,overs,bowler from(
select Match_id,innings,overs,ballnumber,bowler,count(bowler) over(partition by Match_id,innings,overs ) as maiden from IPLBallsDetailsTbl
where batsman_run=0 and extra_type='NA'
)x
where maiden=6
)x
group by bowler
order by 2 desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Most Dot Ball

-- COMMAND ----------

select bowler,count(bowler) as dotBall from IPLBallsDetailsTbl
where batsman_run=0 and extra_type='NA'
group by bowler
order by 2 desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Hattrick

-- COMMAND ----------

create or replace temp view IPLBallsDetailsTemp
as
select t1.Match_id,t1.innings,t1.overs,t1.bowler,ballnumber,isWicketDelivery,row_number() over(partition by t1.Match_id,t1.innings,t1.bowler order by t1.overs,t1.ballnumber) as NoOfball from IPLBallsDetailsTbl t1

where extra_type='NA' and kind not in ('run out','retired hurt','retired out')


-- COMMAND ----------

select bowler,count(*) as NoOfHattrick
from
(
	SELECT distinct t1.Match_id
		,t1.innings
		,t1.overs
		,t1.bowler
        ,t1.isWicketDelivery
        ,t1.NoOfball
        ,t2.NoOfball
        ,t3.NoOfball
	FROM IPLBallsDetailsTemp t1
	INNER JOIN IPLBallsDetailsTemp t2 ON t1.MATCH_ID = t2.MATCH_ID
    	AND t1.innings = t2.innings
        AND t1.bowler = t2.bowler
		AND t1.NoOfball = t2.NoOfball +1
    	INNER JOIN IPLBallsDetailsTemp t3 ON t1.MATCH_ID = t3.MATCH_ID
		AND t1.innings = t3.innings
        AND t1.bowler = t3.bowler
		AND t1.NoOfball = t3.NoOfball +2
where   t1.isWicketDelivery = 1
and t2.isWicketDelivery = 1
and t3.isWicketDelivery = 1
)x
group by bowler
order by 2 desc

-- COMMAND ----------

select Match_id,innings,bowler,sum(total_run) as Runs_Given,sum(isWicketDelivery) as wickets from IPLBallsDetailsTbl
where kind not in ('run out','retired hurt','retired out') and extra_type not in ('legbyes','byes')
group by Match_id,innings,bowler
order by wickets desc,Runs_Given 

-- COMMAND ----------

select bowler,cast(sum(total_run)/sum(isWicketDelivery) as decimal(10,2)) as bowling_Average from IPLBallsDetailsTbl
where kind not in ('run out','retired hurt','retired out') and extra_type not in ('legbyes','byes')
group by bowler
having sum(isWicketDelivery)>20
order by bowling_Average

-- COMMAND ----------


