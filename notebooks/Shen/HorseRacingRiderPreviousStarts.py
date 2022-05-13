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

DataSetName = "RiderPreviousStarts"

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

riderPreviousStartsDF = (
(spark.readStream
  .format("delta")
  .load(SourceDataPath) 
  .select(
    "meetingDate",
    "runnerNumber", 
    "runnerName", 
    "riderOrDriver",
    explode(col("rider_previousStarts")).alias("rider_previousStarts")
  )
).select(
  "meetingDate",
  "runnerNumber", 
  "runnerName", 
  "riderOrDriver",   
  "rider_previousStarts.startType",
  "rider_previousStarts.startDate",
  "rider_previousStarts.raceNumber",
  "rider_previousStarts.finishingPosition",
  "rider_previousStarts.numberOfStarters",
  "rider_previousStarts.draw",
  "rider_previousStarts.margin",
  "rider_previousStarts.venueAbbreviation",
  "rider_previousStarts.distance",
  "rider_previousStarts.class",
  "rider_previousStarts.startingPosition",
  "rider_previousStarts.odds",
  "rider_previousStarts.winnerOrSecond",
  "rider_previousStarts.positionInRun",  
  "rider_previousStarts.time",
  "rider_previousStarts.runner",
  "rider_previousStarts.trainer",  
  "rider_previousStarts.stewardsComment"
  )
)

# COMMAND ----------

(riderPreviousStartsDF.writeStream
  .trigger(once=True)
  .format("delta")
  .outputMode("append")
  .option("checkpointLocation", "%s" % SilverCheckPointPath)
  .start("%s" % SilverDataPath))

# COMMAND ----------

# spark.sql(""" OPTIMIZE delta.`%s` """ % SilverDataPath)
