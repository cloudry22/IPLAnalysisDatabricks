-- Databricks notebook source

create or replace streaming live  table IPL_MATCHES_RAW

comment "Bronze Table"
as
select * from cloud_files("dbfs:/mnt/ipl_data/Matches/","csv",map("cloudFiles.inferColumnTypes","true",
"header", "true",
"quote", '"'
))
