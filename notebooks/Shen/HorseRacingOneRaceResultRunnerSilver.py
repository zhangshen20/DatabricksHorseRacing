# Databricks notebook source
# MAGIC %md 
# MAGIC 
# MAGIC ## Load OneRaceResult data from JSON files into Lake House (Delta)
# MAGIC ### Create 'OneRaceResultRunner' delta

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
TargetDataSetName = "OneRaceResultRunner"

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

oneRaceResultRunnerDF = (
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
  "scratchings",
  explode(col("runners")).alias("runners"),
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
  "scratchings",
  col("runners.runnerName").alias("runnerName"),
  col("runners.runnerNumber").alias("runnerNumber"),
  col("runners.finishingPosition").alias("finishingPosition"),
  col("runners.trainerName").alias("trainerName"),
  col("runners.barrierNumber").alias("barrierNumber"),
  col("runners.riderDriverName").alias("riderDriverName"),
  col("runners.claimAmount").alias("claimAmount"),
  col("runners.fixedOdds.returnWin").alias("fixedOdds_returnWin"),
  col("runners.fixedOdds.returnWinOpen").alias("fixedOdds_returnWinOpen"),    
  col("runners.fixedOdds.returnPlace").alias("fixedOdds_returnPlace"),    
  col("runners.fixedOdds.bettingStatus").alias("fixedOdds_bettingStatus"),    
  col("runners.fixedOdds.winDeduction").alias("fixedOdds_winDeduction"),    
  col("runners.fixedOdds.placeDeduction").alias("fixedOdds_placeDeduction"),    
  col("runners.fixedOdds.propositionNumber").alias("fixedOdds_propositionNumber"),        
  col("runners.fixedOdds.scratchedTime").alias("fixedOdds_scratchedTime"),            
  col("runners.parimutuel.bettingStatus").alias("parimutuel_bettingStatus"),    
  col("runners.parimutuel.returnWin").alias("parimutuel_returnWin"),
  col("runners.parimutuel.returnPlace").alias("parimutuel_returnPlace"),
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

(oneRaceResultRunnerDF.writeStream
  .trigger(once=True)
  .format("delta")
  .outputMode("append")
  .option("checkpointLocation", "%s" % SilverCheckPointPath)
  .start("%s" % SilverDataPath))

# COMMAND ----------

spark.sql(""" OPTIMIZE delta.`%s` """ % SilverDataPath)