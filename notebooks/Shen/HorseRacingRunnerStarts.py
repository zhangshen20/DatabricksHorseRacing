# Databricks notebook source
# MAGIC %md 
# MAGIC 
# MAGIC ## Load RunnerStarts data from JSON files into Lake House (Delta)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Initial Path Setting

# COMMAND ----------

DataSetName = "RunnerStarts"

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
# MAGIC ### runnerStartsDF
# MAGIC #### Load from Bronze into runnerStartsDF
# MAGIC #### Write to SILVER in Delta

# COMMAND ----------

# from pyspark.sql.functions import input_file_name
from pyspark.sql.functions import *

runnerStartsDF = (
(spark.readStream
  .format("delta")
  .load(BronzeDataPath) 
  .select(explode(col("form")).alias("form"), "meetingDate")
).select(
    "meetingDate",
    "form.runnerNumber", 
    "form.runnerName", 
    "form.prizeMoney", 
    "form.last20Starts", 
    "form.sire", 
    "form.dam", 
    "form.age", 
    "form.sex", 
    "form.colour", 
    "form.formComment", 
    "form.formComments",
    "form.classLevel",
    "form.fieldStrength",
    "form.daysSinceLastRun",
    "form.handicapWeight",
    "form.blinkers",
    "form.runsSinceSpell",
    "form.riderOrDriver",
    "form.riderOrDriverSex",
    "form.trainerName",
    "form.trainerLocation",  
    col("form.runnerStarts.startSummaries.overall.numberOfStarts").alias("runnerStarts_overall_numberOfStarts"),
    col("form.runnerStarts.startSummaries.overall.numberOfWins").alias("runnerStarts_overall_numberOfWins"),
    col("form.runnerStarts.startSummaries.overall.numberOfPlacings").alias("runnerStarts_overall_numberOfPlacings"),
    col("form.runnerStarts.startSummaries.track.numberOfStarts").alias("runnerStarts_track_numberOfStarts"),
    col("form.runnerStarts.startSummaries.track.numberOfWins").alias("runnerStarts_track_numberOfWins"),
    col("form.runnerStarts.startSummaries.track.numberOfPlacings").alias("runnerStarts_track_numberOfPlacings"),  
    col("form.runnerStarts.startSummaries.firm.numberOfStarts").alias("runnerStarts_firm_numberOfStarts"),
    col("form.runnerStarts.startSummaries.firm.numberOfWins").alias("runnerStarts_firm_numberOfWins"),
    col("form.runnerStarts.startSummaries.firm.numberOfPlacings").alias("runnerStarts_firm_numberOfPlacings"),  
    col("form.runnerStarts.startSummaries.good.numberOfStarts").alias("runnerStarts_good_numberOfStarts"),
    col("form.runnerStarts.startSummaries.good.numberOfWins").alias("runnerStarts_good_numberOfWins"),
    col("form.runnerStarts.startSummaries.good.numberOfPlacings").alias("runnerStarts_good_numberOfPlacings"),  
    col("form.runnerStarts.startSummaries.dead.numberOfStarts").alias("runnerStarts_dead_numberOfStarts"),
    col("form.runnerStarts.startSummaries.dead.numberOfWins").alias("runnerStarts_dead_numberOfWins"),
    col("form.runnerStarts.startSummaries.dead.numberOfPlacings").alias("runnerStarts_dead_numberOfPlacings"),  
    col("form.runnerStarts.startSummaries.slow.numberOfStarts").alias("runnerStarts_slow_numberOfStarts"),
    col("form.runnerStarts.startSummaries.slow.numberOfWins").alias("runnerStarts_slow_numberOfWins"),
    col("form.runnerStarts.startSummaries.slow.numberOfPlacings").alias("runnerStarts_slow_numberOfPlacings"),  
    col("form.runnerStarts.startSummaries.soft.numberOfStarts").alias("runnerStarts_soft_numberOfStarts"),
    col("form.runnerStarts.startSummaries.soft.numberOfWins").alias("runnerStarts_soft_numberOfWins"),
    col("form.runnerStarts.startSummaries.soft.numberOfPlacings").alias("runnerStarts_soft_numberOfPlacings"),  
    col("form.runnerStarts.startSummaries.heavy.numberOfStarts").alias("runnerStarts_heavy_numberOfStarts"),
    col("form.runnerStarts.startSummaries.heavy.numberOfWins").alias("runnerStarts_heavy_numberOfWins"),
    col("form.runnerStarts.startSummaries.heavy.numberOfPlacings").alias("runnerStarts_heavy_numberOfPlacings"),  
    col("form.runnerStarts.startSummaries.distance.numberOfStarts").alias("runnerStarts_distance_numberOfStarts"),
    col("form.runnerStarts.startSummaries.distance.numberOfWins").alias("runnerStarts_distance_numberOfWins"),
    col("form.runnerStarts.startSummaries.distance.numberOfPlacings").alias("runnerStarts_distance_numberOfPlacings"),    
    col("form.runnerStarts.startSummaries.classSame.numberOfStarts").alias("runnerStarts_classSame_numberOfStarts"),
    col("form.runnerStarts.startSummaries.classSame.numberOfWins").alias("runnerStarts_classSame_numberOfWins"),
    col("form.runnerStarts.startSummaries.classSame.numberOfPlacings").alias("runnerStarts_classSame_numberOfPlacings"),      
    col("form.runnerStarts.startSummaries.classStronger.numberOfStarts").alias("runnerStarts_classStronger_numberOfStarts"),
    col("form.runnerStarts.startSummaries.classStronger.numberOfWins").alias("runnerStarts_classStronger_numberOfWins"),
    col("form.runnerStarts.startSummaries.classStronger.numberOfPlacings").alias("runnerStarts_classStronger_numberOfPlacings"),      
    col("form.runnerStarts.startSummaries.firstUp.numberOfStarts").alias("runnerStarts_firstUp_numberOfStarts"),
    col("form.runnerStarts.startSummaries.firstUp.numberOfWins").alias("runnerStarts_firstUp_numberOfWins"),
    col("form.runnerStarts.startSummaries.firstUp.numberOfPlacings").alias("runnerStarts_firstUp_numberOfPlacings"),      
    col("form.runnerStarts.startSummaries.secondUp.numberOfStarts").alias("runnerStarts_secondUp_numberOfStarts"),
    col("form.runnerStarts.startSummaries.secondUp.numberOfWins").alias("runnerStarts_secondUp_numberOfWins"),
    col("form.runnerStarts.startSummaries.secondUp.numberOfPlacings").alias("runnerStarts_secondUp_numberOfPlacings"),      
    col("form.runnerStarts.startSummaries.trackDistance.numberOfStarts").alias("runnerStarts_trackDistance_numberOfStarts"),
    col("form.runnerStarts.startSummaries.trackDistance.numberOfWins").alias("runnerStarts_trackDistance_numberOfWins"),
    col("form.runnerStarts.startSummaries.trackDistance.numberOfPlacings").alias("runnerStarts_trackDistance_numberOfPlacings"),
    col("form.trainerStarts.startSummaries.track.numberOfStarts").alias("trainerStarts_track_numberOfStarts"),
    col("form.trainerStarts.startSummaries.track.numberOfWins").alias("trainerStarts_track_numberOfWins"),
    col("form.trainerStarts.startSummaries.track.numberOfPlacings").alias("trainerStarts_track_numberOfPlacings"),
    col("form.trainerStarts.startSummaries.region.numberOfStarts").alias("trainerStarts_region_numberOfStarts"),
    col("form.trainerStarts.startSummaries.region.numberOfWins").alias("trainerStarts_region_numberOfWins"),
    col("form.trainerStarts.startSummaries.region.numberOfPlacings").alias("trainerStarts_region_numberOfPlacings"),  
    col("form.trainerStarts.startSummaries.last30Days.numberOfStarts").alias("trainerStarts_last30Days_numberOfStarts"),
    col("form.trainerStarts.startSummaries.last30Days.numberOfWins").alias("trainerStarts_last30Days_numberOfWins"),
    col("form.trainerStarts.startSummaries.last30Days.numberOfPlacings").alias("trainerStarts_last30Days_numberOfPlacings"),  
    col("form.trainerStarts.startSummaries.last12Months.numberOfStarts").alias("trainerStarts_last12Months_numberOfStarts"),
    col("form.trainerStarts.startSummaries.last12Months.numberOfWins").alias("trainerStarts_last12Months_numberOfWins"),
    col("form.trainerStarts.startSummaries.last12Months.numberOfPlacings").alias("trainerStarts_last12Months_numberOfPlacings"),  
    col("form.trainerStarts.startSummaries.jockey.numberOfStarts").alias("trainerStarts_jockey_numberOfStarts"),
    col("form.trainerStarts.startSummaries.jockey.numberOfWins").alias("trainerStarts_jockey_numberOfWins"),
    col("form.trainerStarts.startSummaries.jockey.numberOfPlacings").alias("trainerStarts_jockey_numberOfPlacings"),  
    col("form.riderDriverStarts.startSummaries.track.numberOfStarts").alias("riderDriverStarts_track_numberOfStarts"),
    col("form.riderDriverStarts.startSummaries.track.numberOfWins").alias("riderDriverStarts_track_numberOfWins"),
    col("form.riderDriverStarts.startSummaries.track.numberOfPlacings").alias("riderDriverStarts_track_numberOfPlacings"),
    col("form.riderDriverStarts.startSummaries.region.numberOfStarts").alias("riderDriverStarts_region_numberOfStarts"),
    col("form.riderDriverStarts.startSummaries.region.numberOfWins").alias("riderDriverStarts_region_numberOfWins"),
    col("form.riderDriverStarts.startSummaries.region.numberOfPlacings").alias("riderDriverStarts_region_numberOfPlacings"),  
    col("form.riderDriverStarts.startSummaries.last30Days.numberOfStarts").alias("riderDriverStarts_last30Days_numberOfStarts"),
    col("form.riderDriverStarts.startSummaries.last30Days.numberOfWins").alias("riderDriverStarts_last30Days_numberOfWins"),
    col("form.riderDriverStarts.startSummaries.last30Days.numberOfPlacings").alias("riderDriverStarts_last30Days_numberOfPlacings"),  
    col("form.riderDriverStarts.startSummaries.last12Months.numberOfStarts").alias("riderDriverStarts_last12Months_numberOfStarts"),
    col("form.riderDriverStarts.startSummaries.last12Months.numberOfWins").alias("riderDriverStarts_last12Months_numberOfWins"),
    col("form.riderDriverStarts.startSummaries.last12Months.numberOfPlacings").alias("riderDriverStarts_last12Months_numberOfPlacings"),  
    col("form.riderDriverStarts.startSummaries.runner.numberOfStarts").alias("riderDriverStarts_runner_numberOfStarts"),
    col("form.riderDriverStarts.startSummaries.runner.numberOfWins").alias("riderDriverStarts_runner_numberOfWins"),
    col("form.riderDriverStarts.startSummaries.runner.numberOfPlacings").alias("riderDriverStarts_runner_numberOfPlacings"),
    col("form.runnerStarts.previousStarts").alias("runner_previousStarts"),
    col("form.trainerStarts.previousStarts").alias("trainer_previousStarts"),
    col("form.riderDriverStarts.previousStarts").alias("rider_previousStarts"),
    "form.winningDistances"
    )
)

# COMMAND ----------

(runnerStartsDF.writeStream
  .trigger(once=True)
  .format("delta")
  .outputMode("append")
  .option("checkpointLocation", "%s" % SilverCheckPointPath)
  .start("%s" % SilverDataPath))