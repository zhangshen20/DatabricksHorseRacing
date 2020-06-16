# Databricks notebook source
# MAGIC %md 
# MAGIC 
# MAGIC ## Load RunnerStarts data from JSON files into Lake House (Delta)

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

DataSetName = "RunnerPreviousStarts"

# SILVE SETTING
SilverDataPathBase = "/mnt/gamble/DELTA/SILVER/DATA"
SilverCheckPointPathBase = "/mnt/gamble/DELTA/SILVER/CHECKPOINT"
SilverDataPath = "%s/%s" % (SilverDataPathBase, DataSetName)
SilverCheckPointPath = "%s/%s" % (SilverCheckPointPathBase, DataSetName)
SilverTableName = 'Silver' + DataSetName


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### runnerPreviousStartsDF
# MAGIC #### Load from Bronze into runnerPreviousStartsDF
# MAGIC #### Write to SILVER in Delta

# COMMAND ----------

# from pyspark.sql.functions import input_file_name
from pyspark.sql.functions import *

runnerPreviousStartsDF = (
(spark.readStream
  .format("delta")
  .load(SourceDataPath) 
  .select(
      "meetingDate",
      "runnerNumber", 
      "runnerName", 
      "prizeMoney", 
      "last20Starts",   
      explode(col("runner_previousStarts")).alias("runner_previousStarts")
  )
).select(
  "meetingDate",
  "runnerNumber", 
  "runnerName", 
  "prizeMoney", 
  "last20Starts",   
  "runner_previousStarts.startType",
  "runner_previousStarts.startDate",
  "runner_previousStarts.raceNumber",
  "runner_previousStarts.finishingPosition",
  "runner_previousStarts.numberOfStarters",
  "runner_previousStarts.draw",
  "runner_previousStarts.margin",
  "runner_previousStarts.venueAbbreviation",
  "runner_previousStarts.skyRacing.audio",
  "runner_previousStarts.skyRacing.previewVideo",
  "runner_previousStarts.skyRacing.Video",
  "runner_previousStarts.distance",
  "runner_previousStarts.class",
  "runner_previousStarts.handicap",
  "runner_previousStarts.rider",
  "runner_previousStarts.startingPosition",
  "runner_previousStarts.odds",
  "runner_previousStarts.winnerOrSecond",
  "runner_previousStarts.positionInRun",
  "runner_previousStarts.trackCondition",
  "runner_previousStarts.time",
  "runner_previousStarts.stewardsComment"
  )
)

# COMMAND ----------

(runnerPreviousStartsDF.writeStream
  .trigger(once=True)
  .format("delta")
  .outputMode("append")
  .option("checkpointLocation", "%s" % SilverCheckPointPath)
  .start("%s" % SilverDataPath))