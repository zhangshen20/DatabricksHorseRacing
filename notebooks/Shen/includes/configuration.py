# Databricks notebook source
from pyspark.sql.functions import udf, col, from_unixtime, from_utc_timestamp, from_json
from pyspark.sql.types import StringType, StructField, StructType
import json, time, requests

# COMMAND ----------

MOUNT_NAME = "gamble"
MOUNT_PATH = f"/mnt/{MOUNT_NAME}"

HorseRacingPath = f"{MOUNT_PATH}/HorseRacing"
RunnerStartsDataPath = f"{HorseRacingPath}/JSON"

# BROZNE SETTING 
BronzeDataPathBase = "/mnt/gamble/DELTA/BRONZE/DATA"
BronzeCheckPointPathBase = "/mnt/gamble/DELTA/BRONZE/CHECKPOINT"

# COMMAND ----------

# MAGIC %run ./operations
