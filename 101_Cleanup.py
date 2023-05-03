# Databricks notebook source
# MAGIC %sql
# MAGIC drop DATABASE if exists IPL CASCADE;

# COMMAND ----------

def removeDirectory(dirname):
    files=dbutils.fs.ls(dirname)
    for f in files:
        if f.isDir():
            removeDirectory(f.path)
            dbutils.fs.rm(f.path,True)


# COMMAND ----------

pathList=dbutils.fs.ls("/mnt/IPLData/")

for i in range(0,len(pathList)):
    path=pathList[i][0]
    if "SourceFiles" in path:
        print("Dont Remove {}".format(path))
    else:
        print("Removing {}".format(path))
        removeDirectory(path)
        dbutils.fs.rm(path,True)
