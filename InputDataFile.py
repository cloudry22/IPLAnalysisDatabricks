# Databricks notebook source
# MAGIC %md
# MAGIC ###Files to Read

# COMMAND ----------

Matches="/mnt/ipl_data/IPL_Matches_2008_2022.csv"
BallByBall="/mnt/ipl_data/IPL_Ball_by_Ball_2008_2022.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC ###Write Partition File

# COMMAND ----------

IPLMatches="/mnt/ipl_data/IPL_Matches/"
IPLBallByBall="/mnt/ipl_data/IPL_Ball_by_Ball/"

# COMMAND ----------

# MAGIC %md
# MAGIC ###Create DataFrame on both files

# COMMAND ----------

IPLMatchDetails = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("quote", '"')
    .load(Matches)
)

# COMMAND ----------

IPLBallDetails = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("quote", '"')
    .load(BallByBall)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Write files to seprate directory partition by season

# COMMAND ----------

from pyspark.sql.functions import col
IPLMatchDetails.withColumn("Season_", col("Season")).write.partitionBy('Season_').mode("Overwrite").format("csv").option("header","true").save(IPLMatches)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Balls Details dont have season so we need to add that column first before partitioning

# COMMAND ----------

IPLMatchDetails.createOrReplaceTempView("IPLMatchDetails")
IPLBallDetails.createOrReplaceTempView("IPLBallDetails")

# COMMAND ----------

IPLBallDetailsSeason=spark.sql("select m.Season,b.* from IPLBallDetails b inner join  IPLMatchDetails m on b.id=m.id")

# COMMAND ----------

IPLBallDetailsSeason.withColumn("Season_", col("Season")).write.partitionBy('Season_').mode("Overwrite").format("csv").option("header","true").save(IPLBallByBall)


# COMMAND ----------

# MAGIC %md
# MAGIC ###Function to list the CSV files from partition directory

# COMMAND ----------

def listFile(path):
    list1=dbutils.fs.ls(path)
    FileList=[]
    for i in range(0,len(list1)):
        if "/" in list1[i].name:
            list2=dbutils.fs.ls(path+list1[i].name)
            for j in range(0,len(list2)):
                if ".csv" in list2[j].name:
                    FileList.append(path+list1[i].name+list2[j].name)
    return FileList
                



# COMMAND ----------

IPLMatchesList=listFile(IPLMatches)
IPLBallsList=listFile(IPLBallByBall)

# COMMAND ----------

Matches="/mnt/ipl_data/IPL_Matches_Delta/"
BallByBall="/mnt/ipl_data/IPL_Ball_by_Ball_Delta/"

# COMMAND ----------

dbutils.fs.mkdirs(Matches)
dbutils.fs.mkdirs(BallByBall)

# COMMAND ----------

dbutils.fs.cp(IPLMatchesList[0],Matches)
IPLMatchesList.pop(0)

# COMMAND ----------

dbutils.fs.cp(IPLBallsList[0],BallByBall)
IPLBallsList.pop(0)

# COMMAND ----------

dbutils.fs.rm(BallByBall,True)
dbutils.fs.rm(Matches,True)

# COMMAND ----------

dbutils.fs.ls("dbfs:/mnt/ipl_data/IPL_Matches_Delta/")
