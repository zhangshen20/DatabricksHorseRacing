# Databricks notebook source
# MAGIC %md 
# MAGIC 
# MAGIC ## Load OneRaceResult' data from JSON files into Lake House (Delta)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Initial Path Setting

# COMMAND ----------

MOUNT_NAME = "gamble"
MOUNT_PATH = "/mnt/%s" % MOUNT_NAME

HorseRacingPath = "%s/HorseRacing" % MOUNT_PATH
RunnerStartsDataPath = "%s/JSON" % HorseRacingPath

DataSetName = "OneRaceResult"

# BROZNE SETTING 
BronzeDataPathBase = "/mnt/gamble/DELTA/BRONZE/DATA"
BronzeCheckPointPathBase = "/mnt/gamble/DELTA/BRONZE/CHECKPOINT"
BronzeDataPath = "%s/%s" % (BronzeDataPathBase, DataSetName)
BronzeCheckPointPath = "%s/%s" % (BronzeCheckPointPathBase, DataSetName)
BronzeTableName = 'Bronze' + DataSetName


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Infer JSON File Schema

# COMMAND ----------

meetingDate = '2020-07-25' # THIS CAN BE ANY MEETING DATE

sampleDF = (spark.read.option("inferSchema","true")
          .option("header","true")
          .json("/mnt/gamble/HorseRacing/JSON/%s/Results_OneRace_????-??-??_*.JSON" % meetingDate)
           )

# COMMAND ----------

formSchema = sampleDF.schema

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### formRawDF
# MAGIC #### Load JSON file into formRawDF.
# MAGIC #### Write to Bronze in Delta

# COMMAND ----------

from pyspark.sql.functions import *

formRawDF = (spark.readStream
             .format("json")
             .schema(formSchema)
             .load("/mnt/gamble/HorseRacing/JSON/20??-??-??/Results_OneRace_????-??-??_*.JSON")
             .select(input_file_name().alias("fileName"), regexp_replace(input_file_name(), 'dbfs:/mnt/gamble/HorseRacing/JSON/(\d\d\d\d-\d\d-\d\d)/.*', "$1").alias("meetingDate"), "*")
)

# COMMAND ----------

(formRawDF.writeStream
 .trigger(once=True)
 .format("delta")
 .outputMode("append")
 .option("checkpointLocation", "%s" % BronzeCheckPointPath)
 .start("%s" % BronzeDataPath)
)

# COMMAND ----------

while spark.streams.active != []:
  print("Waiting for streaming '%s' to finish." % BronzeDataPath)
  time.sleep(5)

# COMMAND ----------

# spark.sql(""" OPTIMIZE delta.`%s` """ % BronzeDataPath)
