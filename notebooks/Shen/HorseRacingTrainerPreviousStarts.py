# Databricks notebook source
# MAGIC %md 
# MAGIC 
# MAGIC ## Load TrainerPreviousStarts data from JSON files into Lake House (Delta)

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

DataSetName = "TrainerPreviousStarts"

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

# from pyspark.sql.functions import input_file_name
from pyspark.sql.functions import *

trainerPreviousStartsDF = (
(spark.readStream
  .format("delta")
  .load(SourceDataPath) 
  .select(
    "meetingDate",
    "runnerNumber", 
    "runnerName", 
    "trainerName",
    explode(col("trainer_previousStarts")).alias("trainer_previousStarts")
  )
).select(
  "meetingDate",
  "runnerNumber", 
  "runnerName", 
  "trainerName",
  "trainer_previousStarts.startType",
  "trainer_previousStarts.startDate",
  "trainer_previousStarts.raceNumber",
  "trainer_previousStarts.finishingPosition",
  "trainer_previousStarts.numberOfStarters",
  "trainer_previousStarts.draw",
  "trainer_previousStarts.margin",
  "trainer_previousStarts.venueAbbreviation",
  "trainer_previousStarts.distance",
  "trainer_previousStarts.class",
  "trainer_previousStarts.rider",
  "trainer_previousStarts.startingPosition",
  "trainer_previousStarts.odds",
  "trainer_previousStarts.winnerOrSecond",
  "trainer_previousStarts.positionInRun",  
  "trainer_previousStarts.time",
  "trainer_previousStarts.runner",
  "trainer_previousStarts.stewardsComment"
  )
)

# COMMAND ----------

(trainerPreviousStartsDF.writeStream
  .trigger(once=True)
  .format("delta")
  .outputMode("append")
  .option("checkpointLocation", "%s" % SilverCheckPointPath)
  .start("%s" % SilverDataPath))

# COMMAND ----------

# spark.sql(""" OPTIMIZE delta.`%s` """ % SilverDataPath)