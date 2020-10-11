# Databricks notebook source
# MAGIC %md 
# MAGIC 
# MAGIC ## Load OneRace runner data from OneRace delta into Lake House (Delta)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Initial Path Setting

# COMMAND ----------

SourceDataSetName = "OneRace"
DataSetName = "OneRaceRunner"

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
# MAGIC ### oneRaceRunnerDF
# MAGIC #### Load from Silver OneRace into oneRaceRunnerDF
# MAGIC #### Write to SILVER in Delta

# COMMAND ----------

# from pyspark.sql.functions import input_file_name
from pyspark.sql.functions import *

oneRaceRunnerDF = (
(spark.readStream
  .format("delta")
  .load(SourceDataPath) 
  .select(
  "raceNumber",
  "raceName",
  "meetingName",
  "meetingDate",
  explode(col("runners")).alias("runners"))
).select(
  "raceNumber",
  "raceName",
  "meetingName",
  "meetingDate",
  col("runners.runnerName").alias("runnerName"),
  col("runners.runnerNumber").alias("runnerNumber"), 
  col("runners.fixedOdds.returnWin").alias("fixedOdds_returnWin"),
  col("runners.fixedOdds.returnWinTime").alias("fixedOdds_returnWinTime"),
  col("runners.fixedOdds.returnWinOpen").alias("fixedOdds_returnWinOpen"),
  col("runners.fixedOdds.returnWinOpenDaily").alias("fixedOdds_returnWinOpenDaily"),
  col("runners.fixedOdds.returnPlace").alias("fixedOdds_returnPlace"),
  col("runners.fixedOdds.isFavouriteWin").alias("fixedOdds_isFavouriteWin"),
  col("runners.fixedOdds.isFavouritePlace").alias("fixedOdds_isFavouritePlace"),
  col("runners.fixedOdds.bettingStatus").alias("fixedOdds_bettingStatus"),
  col("runners.fixedOdds.propositionNumber").alias("fixedOdds_propositionNumber"),
  col("runners.fixedOdds.differential").alias("fixedOdds_differential"),
  col("runners.fixedOdds.flucs").alias("fixedOdds_flucs"),
  col("runners.fixedOdds.percentageChange").alias("fixedOdds_percentageChange"),
  col("runners.fixedOdds.allowPlace").alias("fixedOdds_allowPlace"),    
  col("runners.parimutuel.returnWin").alias("parimutuel_returnWin"),
  col("runners.parimutuel.returnPlace").alias("parimutuel_returnPlace"),
  col("runners.parimutuel.returnExact2").alias("parimutuel_returnExact2"),
  col("runners.parimutuel.isFavouriteWin").alias("parimutuel_isFavouriteWin"),
  col("runners.parimutuel.isFavouritePlace").alias("parimutuel_isFavouritePlace"),
  col("runners.parimutuel.isFavouriteExact2").alias("parimutuel_isFavouriteExact2"),
  col("runners.parimutuel.bettingStatus").alias("parimutuel_bettingStatus"),
  col("runners.parimutuel.marketMovers").alias("parimutuel_marketMovers"),
  col("runners.parimutuel.percentageChange").alias("parimutuel_percentageChange"),
  col("runners.silkURL").alias("silkURL"),
  col("runners.trainerName").alias("trainerName"),
  col("runners.vacantBox").alias("vacantBox"),
  col("runners.trainerFullName").alias("trainerFullName"),
  col("runners.barrierNumber").alias("barrierNumber"),
  col("runners.riderDriverName").alias("riderDriverName"),
  col("runners.riderDriverFullName").alias("riderDriverFullName"),
  col("runners.handicapWeight").alias("handicapWeight"),
  col("runners.harnessHandicap").alias("harnessHandicap"),
  col("runners.claimAmount").alias("claimAmount"),
  col("runners.last5Starts").alias("last5Starts"),
  col("runners.tcdwIndicators").alias("tcdwIndicators"),
  col("runners.emergency").alias("emergency"),
  col("runners.penalty").alias("penalty"),
  col("runners.dfsFormRating").alias("dfsFormRating"),
  col("runners.techFormRating").alias("techFormRating"),
  col("runners.totalRatingPoints").alias("totalRatingPoints"),
  col("runners.earlySpeedRating").alias("earlySpeedRating"),
  col("runners.earlySpeedRatingBand").alias("earlySpeedRatingBand")    
)
)

# COMMAND ----------

(oneRaceRunnerDF.writeStream
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

spark.sql(""" OPTIMIZE delta.`%s` """ % SilverDataPath)