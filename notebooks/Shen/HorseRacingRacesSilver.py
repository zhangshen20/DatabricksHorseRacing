# Databricks notebook source
# MAGIC %md 
# MAGIC 
# MAGIC ## Load Races data from JSON files into Lake House (Delta)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Initial Path Setting

# COMMAND ----------

from pyspark.sql.functions import udf, col, from_unixtime, from_utc_timestamp, from_json
from pyspark.sql.types import StringType, StructField, StructType
import json, time, requests

# COMMAND ----------

DataSetName = "Races"

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
# MAGIC ### racesDF
# MAGIC #### Load from Bronze into racesDF
# MAGIC #### Write to SILVER in Delta

# COMMAND ----------

# from pyspark.sql.functions import input_file_name
from pyspark.sql.functions import *

racesDF = (
(spark.readStream
  .format("delta")
  .load(BronzeDataPath) 
  .select(explode(col("races")).alias("races"), "meetingDate")
).select(
  "meetingDate",
  col("races.raceNumber").alias("raceNumber"), 
  col("races.raceName").alias("raceName"),
  col("races.raceClassConditions").alias("raceClassConditions"),
  col("races.raceDistance").alias("raceDistance"),
  col("races.numberOfStarters").alias("numberOfStarters"),
  col("races.isJackpotRace").alias("isJackpotRace"),
  col("races.hasParimutuel").alias("hasParimutuel"),
  col("races.hasFixedOdds").alias("hasFixedOdds"),
  col("races.broadcastChannel").alias("broadcastChannel"),
  col("races.broadcastChannels").alias("broadcastChannels"),
  col("races.willHaveFixedOdds").alias("willHaveFixedOdds"),  
  col("races.allIn").alias("allIn"),
  col("races.cashOutEligibility").alias("cashOutEligibility"),
  col("races.allowBundle").alias("allowBundle"),
  col("races.raceStatus").alias("raceStatus"),
  col("races.results").alias("results"),
  col("races.pools").alias("pools"),
  col("races.skyRacing.audio").alias("skyRacing_audio"),
  col("races.skyRacing.previewVideo").alias("skyRacing_previewVideo")
)
)

# COMMAND ----------

(racesDF.writeStream
  .trigger(once=True)
  .format("delta")
  .outputMode("append")
  .option("checkpointLocation", "%s" % SilverCheckPointPath)
  .start("%s" % SilverDataPath))

# COMMAND ----------

while spark.streams.active != []:
  print("Waiting for streaming '%s' to finish." % SilverDataPath)
  time.sleep(5)

# COMMAND ----------

# spark.sql(""" OPTIMIZE delta.`%s` """ % SilverDataPath)
