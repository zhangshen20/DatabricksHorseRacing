# Databricks notebook source
# MAGIC %md 
# MAGIC 
# MAGIC ## Load Meetings data from JSON files into Lake House (Delta)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Initial Path Setting

# COMMAND ----------

DataSetName = "Meetings"

# BROZNE SETTING 
BronzeDataPathBase = "/mnt/gamble/DELTA/BRONZE/DATA"
BronzeCheckPointPathBase = "/mnt/gamble/DELTA/BRONZE/CHECKPOINT"
BronzeDataPath = "%s/%s" % (BronzeDataPathBase, DataSetName)
BronzeCheckPointPath = "%s/%s" % (BronzeCheckPointPathBase, DataSetName)
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
# MAGIC ### meetingsDF
# MAGIC #### Load from Bronze into meetingsDF
# MAGIC #### Write to SILVER in Delta

# COMMAND ----------

# from pyspark.sql.functions import input_file_name
from pyspark.sql.functions import *

meetingsDF = (
(spark.readStream
  .format("delta")
  .load(BronzeDataPath) 
  .select(explode(col("meetings")).alias("meetings"), "meetingDate")
).select(
  col("meetings.meetingName").alias("meetingName"), 
  col("meetings.location").alias("location"),
  col("meetings.raceType").alias("raceType"),
  col("meetings.meetingDate").alias("meetingDate"),
  col("meetings.prizeMoney").alias("prizeMoney"),
  col("meetings.weatherCondition").alias("weatherCondition"),
  col("meetings.trackCondition").alias("trackCondition"),
  col("meetings.railPosition").alias("railPosition"),
  col("meetings.sellCode.meetingCode").alias("sellCode_meetingCode"),
  col("meetings.sellCode.scheduledType").alias("sellCode_scheduledType"),
  col("meetings.venueMnemonic").alias("venueMnemonic"),
  explode(col("meetings.races")).alias("races")
).select(
  "meetingName",
  "location",
  "raceType",
  "meetingDate",
  "prizeMoney",
  "weatherCondition",
  "trackCondition",
  "railPosition",
  "sellCode_meetingCode",
  "sellCode_scheduledType",
  "venueMnemonic",
  col("races.raceNumber").alias("raceNumber"),
  col("races.raceClassConditions").alias("raceClassConditions"),
  col("races.raceName").alias("raceName"),
  col("races.raceStartTime").alias("raceStartTime"),
  col("races.raceStatus").alias("raceStatus"),
  col("races.raceDistance").alias("raceDistance"),
  col("races.hasParimutuel").alias("hasParimutuel"),
  col("races.hasFixedOdds").alias("hasFixedOdds"),
  col("races.broadcastChannel").alias("broadcastChannel"),
  col("races.broadcastChannels").alias("broadcastChannels"),
  col("races.skyRacing.audio").alias("skyRacing_audio"),
  col("races.skyRacing.video").alias("skyRacing_video"),  
  col("races.willHaveFixedOdds").alias("willHaveFixedOdds"),
  col("races.allIn").alias("allIn"),
  col("races.cashOutEligibility").alias("cashOutEligibility"),
  col("races.allowBundle").alias("allowBundle"),
  col("races.results").alias("results"),
  col("races.scratchings").alias("scratchings")
)
)

# COMMAND ----------

(meetingsDF.writeStream
  .trigger(once=True)
  .format("delta")
  .outputMode("append")
  .option("checkpointLocation", "%s" % SilverCheckPointPath)
  .start("%s" % SilverDataPath))

# COMMAND ----------

# spark.sql(""" OPTIMIZE delta.`%s` """ % SilverDataPath)
