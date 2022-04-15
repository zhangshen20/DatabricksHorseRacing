# Databricks notebook source
# MAGIC %md 
# MAGIC 
# MAGIC ## Load OneRace data from JSON files into Lake House (Delta)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Initial Path Setting

# COMMAND ----------

DataSetName = "OneRace"

# BROZNE SETTING 
BronzeDataPathBase = "/mnt/gamble/DELTA/BRONZE/DATA"
BronzeCheckPointPathBase = "/mnt/gamble/DELTA/BRONZE/CHECKPOINT"
BronzeDataPath = "%s/%s" % (BronzeDataPathBase, DataSetName)
BronzeCheckPointPath = "%s/%s" % (BronzeCheckPointPathBase, "OneRace")
BronzeTableName = 'Bronze' + DataSetName

# SILVE SETTING
SilverDataPathBase = "/mnt/gamble/DELTA/SILVER/DATA"
SilverCheckPointPathBase = "/mnt/gamble/DELTA/SILVER/CHECKPOINT"
SilverDataPath = "%s/%s" % (SilverDataPathBase, DataSetName)
SilverCheckPointPath = "%s/%s" % (SilverCheckPointPathBase, DataSetName)
SilverTableName = 'Silver' + DataSetName


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### oneRaceDF
# MAGIC #### Load from Bronze into oneRaceDF
# MAGIC #### Write to SILVER in Delta

# COMMAND ----------

# from pyspark.sql.functions import input_file_name
from pyspark.sql.functions import *

oneRaceDF = (
spark.readStream
  .format("delta")
  .load(BronzeDataPath) 
  .select(
  "raceNumber",
  "raceName",
  "raceDistance",
  "trackDirection",
  col("meeting.meetingName").alias("meetingName"),
  col("meeting.venueMnemonic").alias("venueMnemonic"),
  col("meeting.meetingDate").alias("meetingDate"),
  col("meeting.location").alias("location"),
  col("meeting.raceType").alias("raceType"),
  col("meeting.sellCode.meetingCode").alias("sellCode_meetingCode"),
  col("meeting.sellCode.scheduledType").alias("sellCode_scheduledType"),
  col("skyRacing.audio").alias("audio"),
  col("skyRacing.previewVideo").alias("previewVideo"),
  "hasParimutuel",
  "hasFixedOdds",
  "broadcastChannel",
  "broadcastChannels",
  "hasForm",
  "hasEarlySpeedRatings",
  "allIn",
  "cashOutEligibility",
  "allowBundle",
  "willHaveFixedOdds",
  "fixedOddsOnlineBetting",
  "raceStartTime",
  "raceClassConditions",
  "apprenticesCanClaim",
  "prizeMoney",
  "raceStatus",
  "substitute",
  "results",
  "pools",
  "allowMulti",
  "allowParimutuelPlace",
  "parimutuelPlaceStatus",
  "allowFixedOddsPlace",
  "numberOfPlaces",
  "numberOfFixedOddsPlaces",
  "runners",
  "oddsUpdateTime",
  "fixedOddsUpdateTime",
  col("tips.tipType").alias("tipType"),
  col("tips.tipster").alias("tipster"),
  col("tips.tipRunnerNumbers").alias("tipRunnerNumbers"),
  "ratings",
  "multiLegApproximates",
  "betTypes",
  "predictions"
)
)

# COMMAND ----------

(oneRaceDF.writeStream
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