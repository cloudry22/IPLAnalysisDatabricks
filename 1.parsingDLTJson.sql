-- Databricks notebook source
create or replace streaming live  table IPL_MATCHES_RAW
TBLPROPERTIES (
   'delta.columnMapping.mode' = 'name',
   'delta.minReaderVersion' = '2',
   'delta.minWriterVersion' = '5')
comment "Bronze Table"
as
select * from cloud_files("/mnt/ipl_data/data/","json",map("cloudFiles.inferColumnTypes","true",
"multiline","true",
"header", "true",
"quote", '"'
))

-- COMMAND ----------

select innings.* from  IPL.IPL_MATCHES_RAW
