# Databricks notebook source
# MAGIC %md 
# MAGIC ##List Databrick Filesystem

# COMMAND ----------

# MAGIC %md 
# MAGIC ##Mount HorseRacing S3 Bucket to DBFS
# MAGIC 
# MAGIC ###It's only required to do ONCE if the cluster is being re-used

# COMMAND ----------

AWS_BUCKET_NAME = "zhangshen20-gamble"
MOUNT_NAME = "gamble"
# dbutils.fs.mount("s3a://%s" % AWS_BUCKET_NAME, "/mnt/%s" % MOUNT_NAME)
display(dbutils.fs.ls("/mnt/%s" % MOUNT_NAME))

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Initial Path Setting

# COMMAND ----------

MOUNT_NAME = "gamble"
MOUNT_PATH = "/mnt/%s" % MOUNT_NAME

HorseRacingPath = "%s/HorseRacing" % MOUNT_PATH

RunnerResultDataPath = "%s/Runner" % HorseRacingPath

# BROZNE SETTING 
BronzeDataPath = "/mnt/gamble/DELTA/BRONZE/DATA"
BronzeCheckPointPath = "/mnt/gamble/DELTA/BRONZE/CHECKPOINT"
BronzeRunnerResultDataPath = "%s/RunnerResult" % BronzeDataPath
BronzeRunnerResultCheckPointPath = "%s/RunnerResult" % BronzeCheckPointPath
BronzeRunnerResultTableName = 'BronzeRunnerResult'

# SILVE SETTING
SilverDataPath = "/mnt/gamble/DELTA/SILVER/DATA"
SilverCheckPointPath = "/mnt/gamble/DELTA/SILVER/CHECKPOINT"
SilverRunnerResultDataPath = "%s/RunnerResult" % SilverDataPath
SilverRunnerResultCheckPointPath = "%s/RunnerResult" % SilverCheckPointPath
SilverRunnerResultTableName = 'SilverRunnerResult'

# GOLD SETTING
GoldDataPath = "/mnt/gamble/DELTA/GOLD/DATA"
GoldCheckPointPath = "/mnt/gamble/DELTA/GOLD/CHECKPOINT"

# print(BronzeCheckPointPath)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ###Create database 'HorseRacing'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC create database if not exists HorseRacing;
# MAGIC 
# MAGIC use HorseRacing;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### BRONZE
# MAGIC #### Define Runner Result Table Schema (Manually)

# COMMAND ----------

BronzeRunnerResultDataSchema = "meetingName STRING, meetingDate STRING, location STRING, weatherCondition STRING, trackCondition STRING, raceNumber STRING, raceName STRING, raceStartTime STRING, raceDistance STRING, trackDirection STRING, raceClassConditions STRING, runnerName STRING, riderName STRING, runnerNumber STRING, finishingPosition STRING, FixedWinOpen STRING, FixedWinClose STRING, overall_runner_starts STRING, track_runner_starts STRING, firm_runner_starts STRING, good_runner_starts STRING, dead_runner_starts STRING, slow_runner_starts STRING, soft_runner_starts STRING, heavy_runner_starts STRING, distance_runner_starts STRING, classSame_runner_starts STRING, classStronger_runner_starts STRING, firstUp_runner_starts STRING, secondUp_runner_starts STRING, trackDistance_runner_starts STRING, overall_runner_wins STRING, track_runner_wins STRING, firm_runner_wins STRING, good_runner_wins STRING, dead_runner_wins STRING, slow_runner_wins STRING, soft_runner_wins STRING, heavy_runner_wins STRING, distance_runner_wins STRING, classSame_runner_wins STRING, classStronger_runner_wins STRING, firstUp_runner_wins STRING, secondUp_runner_wins STRING, trackDistance_runner_wins STRING, overall_runner_placings STRING, track_runner_placings STRING, firm_runner_placings STRING, good_runner_placings STRING, dead_runner_placings STRING, slow_runner_placings STRING, soft_runner_placings STRING, heavy_runner_placings STRING, distance_runner_placings STRING, classSame_runner_placings STRING, classStronger_runner_placings STRING, firstUp_runner_placings STRING, secondUp_runner_placings STRING, trackDistance_runner_placings STRING, track_trainer_starts STRING, region_trainer_starts STRING, last30Days_trainer_starts STRING, last12Months_trainer_starts STRING, jockey_trainer_starts STRING, track_trainer_wins STRING, region_trainer_wins STRING, last30Days_trainer_wins STRING, last12Months_trainer_wins STRING, jockey_trainer_wins STRING, track_trainer_placings STRING, region_trainer_placings STRING, last30Days_trainer_placings STRING, last12Months_trainer_placings STRING, jockey_trainer_placings STRING, track_rider_starts STRING, region_rider_starts STRING, last30Days_rider_starts STRING, last12Months_rider_starts STRING, runner_rider_starts STRING, track_rider_wins STRING, region_rider_wins STRING, last30Days_rider_wins STRING, last12Months_rider_wins STRING, runner_rider_wins STRING, track_rider_placings STRING, region_rider_placings STRING, last30Days_rider_placings STRING, last12Months_rider_placings STRING, runner_rider_placings STRING"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### cloudFiles - Autoloader is incrementally processing files. It is more efficient. However, the Path (load) only can points to a PATH not specific naming convention of files
# MAGIC #### Currently, ../Runner/.. folder contains mix files - Runner_Result, Runner_, Runner_Prefiltered... therefore, Autoloader cannot be used for this case as of now.

# COMMAND ----------

# df = (spark.readStream
#   .format("cloudFiles")
#   .option("cloudFiles.region", "ap-southeast-2")
#   .option("cloudFiles.format", "csv")
#   .option("header", "true")
#   .schema(dataSchema) 
#   .load("/mnt/gamble/TEST/"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### BRZONE
# MAGIC ##### Streaming Read Files from .../Runner/.... Only read file with naming convention 'Runner_Result_20??-??-??.csv'

# COMMAND ----------

df = (spark.readStream
  .format("csv")
  .option("header", "true")
  .schema(BronzeRunnerResultDataSchema) 
  .load("%s/Runner_Result_20??-??-??.csv" % RunnerResultDataPath))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### BRZONE
# MAGIC ##### Write To Bronze Delta Table

# COMMAND ----------

(df.writeStream
  .trigger(once=True)
  .format("delta")
  .outputMode("append")
  .option("checkpointLocation", "%s" % BronzeRunnerResultCheckPointPath)
  .start("%s" % BronzeRunnerResultDataPath))

# COMMAND ----------

display(spark.sql("CREATE TABLE IF NOT EXISTS %s USING DELTA LOCATION '%s'" % (BronzeRunnerResultTableName, BronzeRunnerResultDataPath)))

# COMMAND ----------

spark.sql(""" OPTIMIZE %s """ % BronzeRunnerResultTableName)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### SILVER 
# MAGIC #### Create Silver Table and Load data from Bronze To the Table

# COMMAND ----------

spark.sql("""
create table if not exists %s
using delta
location '%s'
As 
select 
`meetingName` AS `meetingName`	,
 to_date(`meetingDate`, "dd/MM/yyyy hh:mm:ss aa") as meetingDate,
`location` AS `location`	,
`weatherCondition` AS `weatherCondition`	,
`trackCondition` AS `trackCondition`	,
`raceNumber` AS `raceNumber`	,
`raceName` AS `raceName`	,
 to_timestamp(`raceStartTime`, "dd/MM/yyyy hh:mm:ss aa") as raceStartTime,
`raceDistance` AS `raceDistance`	,
`trackDirection` AS `trackDirection`	,
`raceClassConditions` AS `raceClassConditions`	,
`runnerName` AS `runnerName`	,
`riderName` AS `riderName`	,
`runnerNumber` AS `runnerNumber`	,
CAST(`finishingPosition` AS INT) AS `finishingPosition`	,
CAST(`FixedWinOpen` AS INT) AS `FixedWinOpen`	,
CAST(`FixedWinClose` AS INT) AS `FixedWinClose`	,
CAST(`overall_runner_starts` AS INT) AS `overall_runner_starts`	,
CAST(`track_runner_starts` AS INT) AS `track_runner_starts`	,
CAST(`firm_runner_starts` AS INT) AS `firm_runner_starts`	,
CAST(`good_runner_starts` AS INT) AS `good_runner_starts`	,
CAST(`dead_runner_starts` AS INT) AS `dead_runner_starts`	,
CAST(`slow_runner_starts` AS INT) AS `slow_runner_starts`	,
CAST(`soft_runner_starts` AS INT) AS `soft_runner_starts`	,
CAST(`heavy_runner_starts` AS INT) AS `heavy_runner_starts`	,
CAST(`distance_runner_starts` AS INT) AS `distance_runner_starts`	,
CAST(`classSame_runner_starts` AS INT) AS `classSame_runner_starts`	,
CAST(`classStronger_runner_starts` AS INT) AS `classStronger_runner_starts`	,
CAST(`firstUp_runner_starts` AS INT) AS `firstUp_runner_starts`	,
CAST(`secondUp_runner_starts` AS INT) AS `secondUp_runner_starts`	,
CAST(`trackDistance_runner_starts` AS INT) AS `trackDistance_runner_starts`	,
CAST(`overall_runner_wins` AS INT) AS `overall_runner_wins`	,
CAST(`track_runner_wins` AS INT) AS `track_runner_wins`	,
CAST(`firm_runner_wins` AS INT) AS `firm_runner_wins`	,
CAST(`good_runner_wins` AS INT) AS `good_runner_wins`	,
CAST(`dead_runner_wins` AS INT) AS `dead_runner_wins`	,
CAST(`slow_runner_wins` AS INT) AS `slow_runner_wins`	,
CAST(`soft_runner_wins` AS INT) AS `soft_runner_wins`	,
CAST(`heavy_runner_wins` AS INT) AS `heavy_runner_wins`	,
CAST(`distance_runner_wins` AS INT) AS `distance_runner_wins`	,
CAST(`classSame_runner_wins` AS INT) AS `classSame_runner_wins`	,
CAST(`classStronger_runner_wins` AS INT) AS `classStronger_runner_wins`	,
CAST(`firstUp_runner_wins` AS INT) AS `firstUp_runner_wins`	,
CAST(`secondUp_runner_wins` AS INT) AS `secondUp_runner_wins`	,
CAST(`trackDistance_runner_wins` AS INT) AS `trackDistance_runner_wins`	,
CAST(`overall_runner_placings` AS INT) AS `overall_runner_placings`	,
CAST(`track_runner_placings` AS INT) AS `track_runner_placings`	,
CAST(`firm_runner_placings` AS INT) AS `firm_runner_placings`	,
CAST(`good_runner_placings` AS INT) AS `good_runner_placings`	,
CAST(`dead_runner_placings` AS INT) AS `dead_runner_placings`	,
CAST(`slow_runner_placings` AS INT) AS `slow_runner_placings`	,
CAST(`soft_runner_placings` AS INT) AS `soft_runner_placings`	,
CAST(`heavy_runner_placings` AS INT) AS `heavy_runner_placings`	,
CAST(`distance_runner_placings` AS INT) AS `distance_runner_placings`	,
CAST(`classSame_runner_placings` AS INT) AS `classSame_runner_placings`	,
CAST(`classStronger_runner_placings` AS INT) AS `classStronger_runner_placings`	,
CAST(`firstUp_runner_placings` AS INT) AS `firstUp_runner_placings`	,
CAST(`secondUp_runner_placings` AS INT) AS `secondUp_runner_placings`	,
CAST(`trackDistance_runner_placings` AS INT) AS `trackDistance_runner_placings`	,
CAST(`track_trainer_starts` AS INT) AS `track_trainer_starts`	,
CAST(`region_trainer_starts` AS INT) AS `region_trainer_starts`	,
CAST(`last30Days_trainer_starts` AS INT) AS `last30Days_trainer_starts`	,
CAST(`last12Months_trainer_starts` AS INT) AS `last12Months_trainer_starts`	,
CAST(`jockey_trainer_starts` AS INT) AS `jockey_trainer_starts`	,
CAST(`track_trainer_wins` AS INT) AS `track_trainer_wins`	,
CAST(`region_trainer_wins` AS INT) AS `region_trainer_wins`	,
CAST(`last30Days_trainer_wins` AS INT) AS `last30Days_trainer_wins`	,
CAST(`last12Months_trainer_wins` AS INT) AS `last12Months_trainer_wins`	,
CAST(`jockey_trainer_wins` AS INT) AS `jockey_trainer_wins`	,
CAST(`track_trainer_placings` AS INT) AS `track_trainer_placings`	,
CAST(`region_trainer_placings` AS INT) AS `region_trainer_placings`	,
CAST(`last30Days_trainer_placings` AS INT) AS `last30Days_trainer_placings`	,
CAST(`last12Months_trainer_placings` AS INT) AS `last12Months_trainer_placings`	,
CAST(`jockey_trainer_placings` AS INT) AS `jockey_trainer_placings`	,
CAST(`track_rider_starts` AS INT) AS `track_rider_starts`	,
CAST(`region_rider_starts` AS INT) AS `region_rider_starts`	,
CAST(`last30Days_rider_starts` AS INT) AS `last30Days_rider_starts`	,
CAST(`last12Months_rider_starts` AS INT) AS `last12Months_rider_starts`	,
CAST(`runner_rider_starts` AS INT) AS `runner_rider_starts`	,
CAST(`track_rider_wins` AS INT) AS `track_rider_wins`	,
CAST(`region_rider_wins` AS INT) AS `region_rider_wins`	,
CAST(`last30Days_rider_wins` AS INT) AS `last30Days_rider_wins`	,
CAST(`last12Months_rider_wins` AS INT) AS `last12Months_rider_wins`	,
CAST(`runner_rider_wins` AS INT) AS `runner_rider_wins`	,
CAST(`track_rider_placings` AS INT) AS `track_rider_placings`	,
CAST(`region_rider_placings` AS INT) AS `region_rider_placings`	,
CAST(`last30Days_rider_placings` AS INT) AS `last30Days_rider_placings`	,
CAST(`last12Months_rider_placings` AS INT) AS `last12Months_rider_placings`	,
CAST(`runner_rider_placings` AS INT) AS `runner_rider_placings`			
from %s """ % (SilverRunnerResultTableName, SilverRunnerResultDataPath, BronzeRunnerResultTableName));

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### SILVER
# MAGIC ##### Merge New Records to Silver

# COMMAND ----------

spark.sql("""
MERGE INTO %s as Target
USING %s as Source
ON Target.meetingName = Source.meetingName and 
   Target.meetingDate = to_date(Source.meetingDate, "dd/MM/yyyy hh:mm:ss aa") and
   Target.raceNumber  = Source.raceNumber  and
   Target.runnerName  = Source.runnerName  and
   Target.runnerNumber= Source.runnerNumber
WHEN NOT MATCHED
  THEN INSERT 
  (
    `meetingName`,
    `meetingDate`,
    `location`,
    `weatherCondition`,
    `trackCondition`,
    `raceNumber`,
    `raceName`,
    `raceStartTime`,
    `raceDistance`,
    `trackDirection`,
    `raceClassConditions`,
    `runnerName`,
    `riderName`,
    `runnerNumber`,
    `finishingPosition`,
    `FixedWinOpen`,
    `FixedWinClose`,
    `overall_runner_starts`,
    `track_runner_starts`,
    `firm_runner_starts`,
    `good_runner_starts`,
    `dead_runner_starts`,
    `slow_runner_starts`,
    `soft_runner_starts`,
    `heavy_runner_starts`,
    `distance_runner_starts`,
    `classSame_runner_starts`,
    `classStronger_runner_starts`,
    `firstUp_runner_starts`,
    `secondUp_runner_starts`,
    `trackDistance_runner_starts`,
    `overall_runner_wins`,
    `track_runner_wins`,
    `firm_runner_wins`,
    `good_runner_wins`,
    `dead_runner_wins`,
    `slow_runner_wins`,
    `soft_runner_wins`,
    `heavy_runner_wins`,
    `distance_runner_wins`,
    `classSame_runner_wins`,
    `classStronger_runner_wins`,
    `firstUp_runner_wins`,
    `secondUp_runner_wins`,
    `trackDistance_runner_wins`,
    `overall_runner_placings`,
    `track_runner_placings`,
    `firm_runner_placings`,
    `good_runner_placings`,
    `dead_runner_placings`,
    `slow_runner_placings`,
    `soft_runner_placings`,
    `heavy_runner_placings`,
    `distance_runner_placings`,
    `classSame_runner_placings`,
    `classStronger_runner_placings`,
    `firstUp_runner_placings`,
    `secondUp_runner_placings`,
    `trackDistance_runner_placings`,
    `track_trainer_starts`,
    `region_trainer_starts`,
    `last30Days_trainer_starts`,
    `last12Months_trainer_starts`,
    `jockey_trainer_starts`,
    `track_trainer_wins`,
    `region_trainer_wins`,
    `last30Days_trainer_wins`,
    `last12Months_trainer_wins`,
    `jockey_trainer_wins`,
    `track_trainer_placings`,
    `region_trainer_placings`,
    `last30Days_trainer_placings`,
    `last12Months_trainer_placings`,
    `jockey_trainer_placings`,
    `track_rider_starts`,
    `region_rider_starts`,
    `last30Days_rider_starts`,
    `last12Months_rider_starts`,
    `runner_rider_starts`,
    `track_rider_wins`,
    `region_rider_wins`,
    `last30Days_rider_wins`,
    `last12Months_rider_wins`,
    `runner_rider_wins`,
    `track_rider_placings`,
    `region_rider_placings`,
    `last30Days_rider_placings`,
    `last12Months_rider_placings`,
    `runner_rider_placings`
  )
  VALUES (
    Source.`meetingName`,
    to_date(`meetingDate`, "dd/MM/yyyy hh:mm:ss aa"),
    Source.`location`,
    Source.`weatherCondition`,
    Source.`trackCondition`,
    Source.`raceNumber`,
    Source.`raceName`,
    to_timestamp(`raceStartTime`, "dd/MM/yyyy hh:mm:ss aa"),
    Source.`raceDistance`,
    Source.`trackDirection`,
    Source.`raceClassConditions`,
    Source.`runnerName`,
    Source.`riderName`,
    Source.`runnerNumber`,
    CAST(Source.`finishingPosition` AS INT),
    CAST(Source.`FixedWinOpen` AS INT),
    CAST(Source.`FixedWinClose` AS INT),
    CAST(Source.`overall_runner_starts` AS INT),
    CAST(Source.`track_runner_starts` AS INT),
    CAST(Source.`firm_runner_starts` AS INT),
    CAST(Source.`good_runner_starts` AS INT),
    CAST(Source.`dead_runner_starts` AS INT),
    CAST(Source.`slow_runner_starts` AS INT),
    CAST(Source.`soft_runner_starts` AS INT),
    CAST(Source.`heavy_runner_starts` AS INT),
    CAST(Source.`distance_runner_starts` AS INT),
    CAST(Source.`classSame_runner_starts` AS INT),
    CAST(Source.`classStronger_runner_starts` AS INT),
    CAST(Source.`firstUp_runner_starts` AS INT),
    CAST(Source.`secondUp_runner_starts` AS INT),
    CAST(Source.`trackDistance_runner_starts` AS INT),
    CAST(Source.`overall_runner_wins` AS INT),
    CAST(Source.`track_runner_wins` AS INT),
    CAST(Source.`firm_runner_wins` AS INT),
    CAST(Source.`good_runner_wins` AS INT),
    CAST(Source.`dead_runner_wins` AS INT),
    CAST(Source.`slow_runner_wins` AS INT),
    CAST(Source.`soft_runner_wins` AS INT),
    CAST(Source.`heavy_runner_wins` AS INT),
    CAST(Source.`distance_runner_wins` AS INT),
    CAST(Source.`classSame_runner_wins` AS INT),
    CAST(Source.`classStronger_runner_wins` AS INT),
    CAST(Source.`firstUp_runner_wins` AS INT),
    CAST(Source.`secondUp_runner_wins` AS INT),
    CAST(Source.`trackDistance_runner_wins` AS INT),
    CAST(Source.`overall_runner_placings` AS INT),
    CAST(Source.`track_runner_placings` AS INT),
    CAST(Source.`firm_runner_placings` AS INT),
    CAST(Source.`good_runner_placings` AS INT),
    CAST(Source.`dead_runner_placings` AS INT),
    CAST(Source.`slow_runner_placings` AS INT),
    CAST(Source.`soft_runner_placings` AS INT),
    CAST(Source.`heavy_runner_placings` AS INT),
    CAST(Source.`distance_runner_placings` AS INT),
    CAST(Source.`classSame_runner_placings` AS INT),
    CAST(Source.`classStronger_runner_placings` AS INT),
    CAST(Source.`firstUp_runner_placings` AS INT),
    CAST(Source.`secondUp_runner_placings` AS INT),
    CAST(Source.`trackDistance_runner_placings` AS INT),
    CAST(Source.`track_trainer_starts` AS INT),
    CAST(Source.`region_trainer_starts` AS INT),
    CAST(Source.`last30Days_trainer_starts` AS INT),
    CAST(Source.`last12Months_trainer_starts` AS INT),
    CAST(Source.`jockey_trainer_starts` AS INT),
    CAST(Source.`track_trainer_wins` AS INT),
    CAST(Source.`region_trainer_wins` AS INT),
    CAST(Source.`last30Days_trainer_wins` AS INT),
    CAST(Source.`last12Months_trainer_wins` AS INT),
    CAST(Source.`jockey_trainer_wins` AS INT),
    CAST(Source.`track_trainer_placings` AS INT),
    CAST(Source.`region_trainer_placings` AS INT),
    CAST(Source.`last30Days_trainer_placings` AS INT),
    CAST(Source.`last12Months_trainer_placings` AS INT),
    CAST(Source.`jockey_trainer_placings` AS INT),
    CAST(Source.`track_rider_starts` AS INT),
    CAST(Source.`region_rider_starts` AS INT),
    CAST(Source.`last30Days_rider_starts` AS INT),
    CAST(Source.`last12Months_rider_starts` AS INT),
    CAST(Source.`runner_rider_starts` AS INT),
    CAST(Source.`track_rider_wins` AS INT),
    CAST(Source.`region_rider_wins` AS INT),
    CAST(Source.`last30Days_rider_wins` AS INT),
    CAST(Source.`last12Months_rider_wins` AS INT),
    CAST(Source.`runner_rider_wins` AS INT),
    CAST(Source.`track_rider_placings` AS INT),
    CAST(Source.`region_rider_placings` AS INT),
    CAST(Source.`last30Days_rider_placings` AS INT),
    CAST(Source.`last12Months_rider_placings` AS INT),
    CAST(Source.`runner_rider_placings` AS INT)
  ) """ % (SilverRunnerResultTableName, BronzeRunnerResultTableName));
  

# COMMAND ----------

# MAGIC %md
# MAGIC #### SILVER
# MAGIC ##### Optimize Silver Table

# COMMAND ----------

spark.sql(""" OPTIMIZE %s """ % SilverRunnerResultTableName)