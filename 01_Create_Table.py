# Databricks notebook source
# MAGIC %md
# MAGIC ## Read Files and infer Schema

# COMMAND ----------

IPLMatchDetails = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("quote", '"')
    .load(
        "/mnt/ipl_data/IPL_Matches_2008_2022.csv"
    )
)

# COMMAND ----------

IPLBallsDetails = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("quote", '"')
    .load(
        "/mnt/ipl_data/IPL_Ball_by_Ball_2008_2022.csv"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ###Transform Dataframes

# COMMAND ----------

from pyspark.sql.functions import col, split ,regexp_replace

IPLMatchDetailsTrans=IPLMatchDetails.withColumn("Team1Players",regexp_replace(col("Team1Players"),"\\[|\\]|\\'",""))\
                                 .withColumn("Team1Players",regexp_replace(col("Team1Players"),"\s*,\s*",","))\
                                 .withColumn("Team1Players",split(col("Team1Players"),","))\
                                 .withColumn("Team2Players",regexp_replace(col("Team2Players"),"\\[|\\]|\\'",""))\
                                 .withColumn("Team2Players",regexp_replace(col("Team2Players"),"\s*,\s*",","))\
                                 .withColumn("Team2Players",split(col("Team2Players"),","))

# COMMAND ----------

IPLBallsDetailsTrans=IPLBallsDetails.withColumnRenamed("non-striker","non_striker")\
                                  .withColumnRenamed("id","MATCH_ID")

# COMMAND ----------

IPLMatchDetailsTrans.createOrReplaceTempView("IPLMatchDetailsTbl")
IPLBallsDetailsTrans.createOrReplaceTempView("IPLBallsDetailsTbl")

# COMMAND ----------

# MAGIC %sql
# MAGIC create database  if not exists IPL;
# MAGIC use IPL;

# COMMAND ----------

# MAGIC %sql
# MAGIC Create or replace table IPLMatchDetailsTbl
# MAGIC select * from IPLMatchDetailsTbl

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC Create or replace table IPLBallsDetailsTbl
# MAGIC select * from IPLBallsDetailsTbl

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view IPL_MATCHES using CSV options(
# MAGIC path="/mnt/ipl_data/IPL_Matches_2008_2022.csv",
# MAGIC header="true",
# MAGIC mode="failfast",
# MAGIC inferSchema="true",
# MAGIC quote='"'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from IPL.IPL_MATCHES_RAW
