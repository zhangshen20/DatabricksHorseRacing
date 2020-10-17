# Databricks notebook source
# MAGIC %md 
# MAGIC 
# MAGIC ## Load RunnerStarts data from JSON files into Lake House (Delta)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Initial Path Setting

# COMMAND ----------

from pyspark.sql.functions import udf, col, from_unixtime, from_utc_timestamp, from_json
from pyspark.sql.types import StringType, StructField, StructType
import json, time, requests

# COMMAND ----------

MOUNT_NAME = "gamble"
MOUNT_PATH = "/mnt/%s" % MOUNT_NAME

HorseRacingPath = "%s/HorseRacing" % MOUNT_PATH
RunnerStartsDataPath = "%s/JSON" % HorseRacingPath

DataSetName = "RunnerStarts"

# BROZNE SETTING 
BronzeDataPathBase = "/mnt/gamble/DELTA/BRONZE/DATA"
BronzeCheckPointPathBase = "/mnt/gamble/DELTA/BRONZE/CHECKPOINT"
BronzeDataPath = "%s/%s" % (BronzeDataPathBase, DataSetName)
BronzeCheckPointPath = "%s/%s" % (BronzeCheckPointPathBase, "RunnerStartsV2")
BronzeTableName = 'Bronze' + DataSetName


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Infer JSON File Schema

# COMMAND ----------

meetingDate = '2020-06-03' # THIS CAN BE ANY MEETING DATE

sampleDF = (spark.read.option("inferSchema","true")
          .option("header","true")
          .json("/mnt/gamble/HorseRacing/JSON/Databricks/OneRaceForm/OneRace_Form_2020-10-17_CAULFIELD_9_R.JSON")
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
             .load("/mnt/gamble/HorseRacing/JSON/Databricks/OneRaceForm/")
             .select(input_file_name().alias("fileName"), regexp_replace(input_file_name(), 'dbfs:/mnt/gamble/HorseRacing/JSON/Databricks/OneRaceForm/OneRace_Form_(\d\d\d\d-\d\d-\d\d)_.*.JSON', "$1").alias("meetingDate"), "*")
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
  print("Waiting for streaming query to finish.")
  time.sleep(5)

# COMMAND ----------

spark.sql(""" OPTIMIZE delta.`%s` """ % BronzeDataPath)