# Databricks notebook source
# MAGIC %md 
# MAGIC 
# MAGIC ## Load RunnerStarts data from JSON files into Lake House (Delta)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Initial Path Setting

# COMMAND ----------

from pyspark.sql.functions import udf, col, from_unixtime, from_utc_timestamp, from_json
from pyspark.sql.types import StringType, StructField, StructType
import json, time, requests

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

# ----------------------------------------------------------

DataSetNameUnique = "RunnerPreviousStartsUnique"

# SILVER SETTING
SilverDataPathUnique = "%s/%s" % (SilverDataPathBase, DataSetNameUnique)
SilverCheckPointPathUnique = "%s/%s" % (SilverCheckPointPathBase, DataSetNameUnique)
SilverTableNameUnique = 'Silver' + DataSetNameUnique


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### runnerPreviousStartsDF
# MAGIC #### Load from Bronze into runnerPreviousStartsDF
# MAGIC #### Write to SILVER in Delta

# COMMAND ----------

# from pyspark.sql.functions import input_file_name
from pyspark.sql.functions import *

(
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
).writeStream
 .trigger(once=True)
 .format("delta")
 .outputMode("append")
 .option("checkpointLocation", "%s" % SilverCheckPointPath)
 .start("%s" % SilverDataPath)
)

# COMMAND ----------

# (runnerPreviousStartsDF.writeStream
#   .trigger(once=True)
#   .format("delta")
#   .outputMode("append")
#   .option("checkpointLocation", "%s" % SilverCheckPointPath)
#   .start("%s" % SilverDataPath))

# COMMAND ----------

while spark.streams.active != []:
  print("Waiting for streaming query to finish.")
  time.sleep(5)

# COMMAND ----------

spark.sql(""" OPTIMIZE delta.`%s` """ % SilverDataPath)

# COMMAND ----------

(spark.readStream
  .format("delta")
  .load(SilverDataPath)
  .createOrReplaceTempView("runner_previous_starts_temp"))

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TEMP VIEW runner_previous_starts_unique_temp AS (
# MAGIC   SELECT venueAbbreviation, startDate, raceNumber, runnerName, last(prizeMoney) as prizeMoney, last(last20Starts) as last20Starts, last(startType) as startType,
# MAGIC          last(finishingPosition) as finishingPosition, last(numberOfStarters) as numberOfStarters, last(draw) as draw, last(margin) as margin, last(audio) as audio,
# MAGIC          last(previewVideo) as previewVideo, last(Video) as Video, min(distance) as distance, last(class) as class, last(handicap) as handicap, last(rider) as rider,
# MAGIC          last(startingPosition) as startingPosition, last(odds) as odds, last(winnerOrSecond) as winnerOrSecond, last(positionInRun) as positionInRun,
# MAGIC          last(trackCondition) as trackCondition, last(time) as time, last(stewardsComment) as stewardsComment
# MAGIC   FROM runner_previous_starts_temp
# MAGIC   GROUP BY venueAbbreviation, startDate, raceNumber, runnerName
# MAGIC )

# COMMAND ----------

(spark.table("runner_previous_starts_unique_temp")
  .writeStream
  .format("delta")
  .outputMode("complete")
  .option("checkpointLocation", SilverCheckPointPathUnique)
  .trigger(once=True)
  .start(SilverDataPathUnique)
)

# COMMAND ----------

while spark.streams.active != []:
  print("Waiting for streaming query to finish.")
  time.sleep(5)

# COMMAND ----------

spark.sql(""" OPTIMIZE delta.`%s` """ % SilverDataPathUnique)