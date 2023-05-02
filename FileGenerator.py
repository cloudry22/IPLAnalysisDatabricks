# Databricks notebook source
# MAGIC %run /Repos/raut1606@gmail.com/IPLAnalysisDatabricks/00_Setup

# COMMAND ----------

for i in range(1000):
     try:
        CopyFile()
     except:
        print(str(e))
    
