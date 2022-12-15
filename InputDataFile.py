# Databricks notebook source
IPLMatchDetails = spark.read.format("csv").option("header","true").option("quote", "\"").load("dbfs:/FileStore/shared_uploads/praut1606@gmail.com/IPL_Matches_2008_2022.csv")
