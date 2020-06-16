# Databricks notebook source
# MAGIC %md 
# MAGIC 
# MAGIC ## Load RiderPreviousStarts data from JSON files into Lake House (Delta)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Initial Path Setting

# COMMAND ----------

# SOURCE SETTING 
SourceDataSetName = "RunnerStarts"
SourceDataPathBase = "/mnt/gamble/DELTA/SILVER/DATA"
SourceDataPath = "%s/%s" % (SourceDataPathBase, SourceDataSetName)

# ----------------------------------------------------------

DataSetName = "RunnerWinningDistance"

# SILVE SETTING
SilverDataPathBase = "/mnt/gamble/DELTA/SILVER/DATA"
SilverCheckPointPathBase = "/mnt/gamble/DELTA/SILVER/CHECKPOINT"
SilverDataPath = "%s/%s" % (SilverDataPathBase, DataSetName)
SilverCheckPointPath = "%s/%s" % (SilverCheckPointPathBase, DataSetName)
SilverTableName = 'Silver' + DataSetName


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### trainerPreviousStartsDF
# MAGIC #### Load from Bronze into trainerPreviousStartsDF
# MAGIC #### Write to SILVER in Delta

# COMMAND ----------

from pyspark.sql.functions import *

runnerWinningDistanceDF = (
(spark.readStream
  .format("delta")
  .load(SourceDataPath) 
  .select(
    "meetingDate",
    "runnerNumber", 
    "runnerName",   
    explode(col("winningDistances")).alias("winningDistances")
  )
).select(
  "meetingDate",
  "runnerNumber", 
  "runnerName", 
  "winningDistances.winningDistance",
  "winningDistances.numberOfWinsAtDistance"
  )
)

# COMMAND ----------

(runnerWinningDistanceDF.writeStream
  .trigger(once=True)
  .format("delta")
  .outputMode("append")
  .option("checkpointLocation", "%s" % SilverCheckPointPath)
  .start("%s" % SilverDataPath))