# Databricks notebook source
SourceLocation="/mnt/ipl_data/data/Source/"
SourceCompleteLocation="/mnt/ipl_data/data/Source_Complete/"
TargetLocationMatches="/mnt/ipl_data/data/TargetMatches/"
TargetLocationBallByBall="/mnt/ipl_data/data/TargetBallByBall/"

SchemaLocation="/mnt/ipl_data/Schema/"
CheckpointLocation="/mnt/ipl_data/CheckPoint/"


# COMMAND ----------

def cleanup(directory):
    if directory:
       dbutils.fs.rm(directory,True)
    else:
        dbutils.fs.mkdirs(directory)

# COMMAND ----------

dbutils.fs.mkdirs(SourceLocation)

# COMMAND ----------

def listFile(path):
    list1=dbutils.fs.ls(path)
    FileList=[]
    for i in range(0,len(list1)):
        if "/" in list1[i].path:
            list2=dbutils.fs.ls(path+list1[i].name)
            for j in range(0,len(list2)):
                if ".json" in list2[j].name:
                    FileList.append(path+list2[j].name)
    return FileList
                



# COMMAND ----------

FileList=listFile(SourceCompleteLocation)

# COMMAND ----------

def CopyFile():
    sourcePath=FileList[0]
    dbutils.fs.cp(sourcePath,SourceLocation)
    print("File ",{sourcePath},"Copied to ",{SourceLocation})
 
    FileList.pop(0)

# COMMAND ----------

CopyFile()
