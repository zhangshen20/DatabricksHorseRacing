# Databricks notebook source
# MAGIC %md 
# MAGIC 
# MAGIC ## Load OneRaceResult data from JSON files into Lake House (Delta)
# MAGIC ### Create 'OneRaceResultScratchings' delta

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Initial Path Setting

# COMMAND ----------

# BROZNE SETTING 
SourceDataSetName = "OneRaceResult"

BronzeDataPathBase = "/mnt/gamble/DELTA/BRONZE/DATA"
BronzeCheckPointPathBase = "/mnt/gamble/DELTA/BRONZE/CHECKPOINT"
BronzeDataPath = "%s/%s" % (BronzeDataPathBase, SourceDataSetName)
BronzeCheckPointPath = "%s/%s" % (BronzeCheckPointPathBase, SourceDataSetName)
BronzeTableName = 'Bronze' + SourceDataSetName

# SILVE SETTING
TargetDataSetName = "OneRaceResultScratching"

SilverDataPathBase = "/mnt/gamble/DELTA/SILVER/DATA"
SilverCheckPointPathBase = "/mnt/gamble/DELTA/SILVER/CHECKPOINT"
SilverDataPath = "%s/%s" % (SilverDataPathBase, TargetDataSetName)
SilverCheckPointPath = "%s/%s" % (SilverCheckPointPathBase, TargetDataSetName)
SilverTableName = 'Silver' + TargetDataSetName


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### oneRaceResultDF
# MAGIC #### Load from Bronze into oneRaceResultDF
# MAGIC #### Write to SILVER in Delta

# COMMAND ----------

# from pyspark.sql.functions import input_file_name
from pyspark.sql.functions import *

oneRaceResultScracthingDF = (
spark.readStream
  .format("delta")
  .load(BronzeDataPath) 
  .select(
  "raceNumber",
  "raceName",
  "raceStartTime",
  "raceStatus",
  "raceDistance",
  "results",
  "resultedTime",
  "substitute",
  explode(col("scratchings")).alias("scratchings"),    
  "runners",
  "raceClassConditions",
  "hasFixedOdds",
  "hasParimutuel",
  "winBook",
  "willHaveFixedOdds",
  col("meeting.meetingDate").alias("meetingDate"),
  col("meeting.location").alias("location"),
  col("meeting.meetingName").alias("meetingName"),
  col("meeting.raceType").alias("raceType"),
  col("meeting.venueMnemonic").alias("venueMnemonic"),    
  col("meeting.sellCode.meetingCode").alias("sellCode_meetingCode"),    
  col("meeting.sellCode.scheduledType").alias("sellCode_scheduledType"),        
  col("skyRacing.audio").alias("skyRacing_audio"),
  "pools",
  "dividends"
).select(
  "raceNumber",
  "raceName",
  "raceStartTime",
  "raceStatus",
  "raceDistance",
  "results",
  "resultedTime",
  "substitute",
  col("scratchings.runnerName").alias("runnerName"),
  col("scratchings.runnerNumber").alias("runnerNumber"),
  col("scratchings.finishingPosition").alias("finishingPosition"),
  col("scratchings.trainerName").alias("trainerName"),
  col("scratchings.barrierNumber").alias("barrierNumber"),
  col("scratchings.riderDriverName").alias("riderDriverName"),
  col("scratchings.claimAmount").alias("claimAmount"),
  col("scratchings.fixedOdds.returnWin").alias("fixedOdds_returnWin"),
  col("scratchings.fixedOdds.returnWinOpen").alias("fixedOdds_returnWinOpen"),    
  col("scratchings.fixedOdds.returnPlace").alias("fixedOdds_returnPlace"),    
  col("scratchings.fixedOdds.bettingStatus").alias("fixedOdds_bettingStatus"),    
  col("scratchings.fixedOdds.winDeduction").alias("fixedOdds_winDeduction"),    
  col("scratchings.fixedOdds.placeDeduction").alias("fixedOdds_placeDeduction"),    
  col("scratchings.fixedOdds.propositionNumber").alias("fixedOdds_propositionNumber"),        
  col("scratchings.fixedOdds.scratchedTime").alias("fixedOdds_scratchedTime"),            
  col("scratchings.parimutuel.bettingStatus").alias("parimutuel_bettingStatus"),    
  col("scratchings.parimutuel.returnWin").alias("parimutuel_returnWin"),
  col("scratchings.parimutuel.returnPlace").alias("parimutuel_returnPlace"),
  "runners",
  "raceClassConditions",
  "hasFixedOdds",
  "hasParimutuel",
  "winBook",
  "willHaveFixedOdds",
  "meetingDate",
  "location",
  "meetingName",
  "raceType",
  "venueMnemonic",
  "sellCode_meetingCode",
  "sellCode_scheduledType",
  "skyRacing_audio",
  "pools",
  "dividends"    
)
)

# COMMAND ----------

(oneRaceResultScracthingDF.writeStream
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