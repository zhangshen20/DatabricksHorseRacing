# Databricks notebook source
# MAGIC %md 
# MAGIC 
# MAGIC ## Load OneRace Ratings data from OneRace delta into Lake House (Delta)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Initial Path Setting

# COMMAND ----------

SourceDataSetName = "OneRace"
DataSetName = "OneRaceRatings"

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
# MAGIC ### oneRaceRatingsDF
# MAGIC #### Load from Silver OneRace into oneRaceRatingsDF
# MAGIC #### Write to SILVER in Delta

# COMMAND ----------

# from pyspark.sql.functions import input_file_name
from pyspark.sql.functions import *

oneRaceRatingsDF = (
(spark.readStream
  .format("delta")
  .load(SourceDataPath) 
  .select(
  "raceNumber",
  "raceName",
  "meetingName",
  "meetingDate",
  explode(col("ratings")).alias("ratings"))
).select(
  "raceNumber",
  "raceName",
  "meetingName",
  "meetingDate",
  col("ratings.ratingType").alias("ratingType"),
  col("ratings.ratingRunnerNumbers").alias("ratingRunnerNumbers")
)
)

# COMMAND ----------

(oneRaceRatingsDF.writeStream
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
