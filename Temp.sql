-- Databricks notebook source
SELECT DISTINCT ID from IPL.ipl_details

-- COMMAND ----------

ALTER TABLE IPL.IPL_Deatils
rename   to IPL.IPL_Details

-- COMMAND ----------

drop table  BallByBallDetails
