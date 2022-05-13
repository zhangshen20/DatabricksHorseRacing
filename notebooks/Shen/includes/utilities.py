# Databricks notebook source
from pyspark.sql.session import SparkSession
from urllib.request import urlretrieve
import time

def stop_all_streams() -> bool:
    stopped = False
    for stream in spark.streams.active:
        stopped = True
        stream.stop()
    return stopped

def stop_named_stream(spark: SparkSession, namedStream: str) -> bool:
    stopped = False
    for stream in spark.streams.active:
        if stream.name == namedStream:
            stopped = True
            stream.stop()
    return stopped
  
def until_stream_is_ready(namedStream: str, progressions: int = 3) -> bool:
    queries = list(filter(lambda query: query.name == namedStream, spark.streams.active))
    while len(queries) == 0 or len(queries[0].recentProgress) < progressions:
        time.sleep(5)
        queries = list(filter(lambda query: query.name == namedStream, spark.streams.active))
    print("The stream {} is active and ready.".format(namedStream))
    return True
  
  

# COMMAND ----------

# This function only works in one Spark session
def until_all_streams_finished() -> bool:
  while spark.streams.active != []:
    for stream in spark.streams.active:
      print(f"Waiting for streaming {stream.name} to finish.")
    time.sleep(5)    
