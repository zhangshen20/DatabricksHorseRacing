# Databricks notebook source
# MAGIC %md 
# MAGIC 
# MAGIC ## Load OneRace Predictions data from OneRace delta into Lake House (Delta)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Initial Path Setting

# COMMAND ----------

SourceDataSetName = "OneRace"
DataSetName = "OneRacePredictions"

# SOURCE SETTING 
SourceDataPathBase = "/mnt/gamble/DELTA/SILVER/DATA"
SourceCheckPointPathBase = "/mnt/gamble/DELTA/SILVER/CHECKPOINT"
SourceDataPath = "%s/%s" % (SourceDataPathBase, SourceDataSetName)
SourceCheckPointPath = "%s/%s" % (SourceCheckPointPathBase, SourceDataSetName)
SourceTableName = 'Source' + SourceDataSetName

# SILVE SETTING
SilverDataPathBase = "/mnt/gamble/DELTA/SILVER/DATA"
SilverCheckPointPathBase = "/mnt/gamble/DELTA/SILVER/CHECKPOINT"
SilverDataPath = "%s/%s" % (SilverDataPathBase, DataSetName)
SilverCheckPointPath = "%s/%s" % (SilverCheckPointPathBase, DataSetName)
SilverTableName = 'Silver' + DataSetName


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### oneRacePredictionsDF
# MAGIC #### Load from Silver OneRace into oneRacePredictionsDF
# MAGIC #### Write to SILVER in Delta

# COMMAND ----------

# from pyspark.sql.functions import input_file_name
from pyspark.sql.functions import *

oneRacePredictionsDF = (
(spark.readStream
  .format("delta")
  .load(SourceDataPath) 
  .select(
  "raceNumber",
  "raceName",
  "meetingName",
  "meetingDate",
  explode(col("predictions")).alias("predictions"))
).select(
  "raceNumber",
  "raceName",
  "meetingName",
  "meetingDate",
  col("predictions.betType").alias("predictions_betType"),
  explode(col("predictions.runners")).alias("predictions_runners")
).select(
  "raceNumber",
  "raceName",
  "meetingName",
  "meetingDate",
  "predictions_betType",
  col("predictions_runners.runnerNumber").alias("predictions_runners_runnerNumber"),
  col("predictions_runners.probability").alias("predictions_runners_probability"),
  col("predictions_runners.runnerName").alias("predictions_runners_runnerName")
)
)

# COMMAND ----------

(oneRacePredictionsDF.writeStream
  .trigger(once=True)
  .format("delta")
  .outputMode("append")
  .option("checkpointLocation", "%s" % SilverCheckPointPath)
  .start("%s" % SilverDataPath))

# COMMAND ----------

import json, time, requests

while spark.streams.active != []:
  print("Waiting for streaming '%s' to finish." % SilverDataPath)
  time.sleep(5)

# COMMAND ----------

# spark.sql(""" OPTIMIZE delta.`%s` """ % SilverDataPath)
