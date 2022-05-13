# Databricks notebook source
# MAGIC %md 
# MAGIC 
# MAGIC ## Load OneRace' data from JSON files into Lake House (Delta)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Initial Path Setting

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

OneRaceBronzeDataSetName = "OneRace"

BronzeDataPath = f"{BronzeDataPathBase}/{OneRaceBronzeDataSetName}"
BronzeCheckPointPath = f"{BronzeCheckPointPathBase}/OneRace"
BronzeTableName = 'Tbl_Bronze' + OneRaceBronzeDataSetName

# COMMAND ----------

SourcePath = f"{RunnerStartsDataPath}/Databricks/OneRaceSelf"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Infer JSON File Schema

# COMMAND ----------

# ! only one sameple file is not good enough. Due to inferred data type could be different from actual delta table. 
# a number of sample files are required if you want to do 'Infer' schema from JSON files
# i.e. runner.claimsAmount in Table is 'double', however, in some Json files, long type is inferred 

sampleDF = read_json_schema(spark, f"{SourcePath}/OneRace_Self_2020-10-17_*_R.JSON")

# COMMAND ----------

formSchema = sampleDF.schema

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### formRawDF
# MAGIC #### Load JSON file into formRawDF.
# MAGIC #### Write to Bronze in Delta

# COMMAND ----------

formRawDF = read_stream_json(spark, SourcePath, formSchema)

# COMMAND ----------

formRawDF = (formRawDF
             .select("*", regexp_replace("fileName", f"dbfs:{SourcePath}/OneRace_Self_(\d\d\d\d-\d\d-\d\d)_.*.JSON", "$1").alias("meetingDate"))
            )

# COMMAND ----------

formRawWriter = create_stream_writer(
  dataframe=formRawDF,
  checkpoint=BronzeCheckPointPath,
  name="write_raw_to_bronze_oneRace",
  partition_column="meetingDate"
)

# COMMAND ----------

formRawWriter = formRawWriter.trigger(once=True)

# COMMAND ----------

formRawWriter.start(BronzeDataPath)

# COMMAND ----------

# allStreamFinished = until_all_streams_finished()

# if allStreamFinished:
#   print(f"HorseRacingOneRaceBronze is finished")

# COMMAND ----------

while spark.streams.active != []:
  print("Waiting for streaming query to finish.")
  time.sleep(5)

# COMMAND ----------

# spark.sql(""" OPTIMIZE delta.`%s` """ % BronzeDataPath)