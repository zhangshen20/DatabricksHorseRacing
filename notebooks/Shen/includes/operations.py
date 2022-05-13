# Databricks notebook source
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    current_timestamp,
    from_json,
    from_unixtime,
    lag,
    lead,
    lit,
    mean,
    stddev,
    max,
    input_file_name,
    regexp_replace,
)
from pyspark.sql.session import SparkSession
from pyspark.sql.streaming import DataStreamWriter
from pyspark.sql.window import Window
from pyspark.sql.types import _parse_datatype_string, StructType, BooleanType

# COMMAND ----------

def read_json_schema(spark: SparkSession, jsonPath: str) -> DataFrame:
  return (spark.read.option("inferSchema","true")
          .option("header","true")
          .json(jsonPath)
          )

# COMMAND ----------

def read_stream_json(spark: SparkSession, jsonPath: str, schema: StructType) -> DataFrame:
 
  return(spark.readStream
             .format("json")
             .schema(schema)
             .load(jsonPath)
             .select(input_file_name().alias("fileName"), "*")
    )
  

# COMMAND ----------

def create_stream_writer(
    dataframe: DataFrame,
    checkpoint: str,
    name: str,
    partition_column: str,
    mode: str = "append",
    mergeSchema: bool = False,
) -> DataStreamWriter:

    stream_writer = (
        dataframe.writeStream
        .format("delta")
        .outputMode(mode)
        .option("checkpointLocation", checkpoint)
        .partitionBy(partition_column)
        .queryName(name)
    )

    if mergeSchema:
        stream_writer = stream_writer.option("mergeSchema", True)
        
    if partition_column is not None:
        stream_writer = stream_writer.partitionBy(partition_column)
        
    return stream_writer

# COMMAND ----------

def change_partition_column(
  dataframe: DataFrame,
  partition_column: str,
  table_name: str
) -> BooleanType:
  (dataframe.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy(partition_column)
    .saveAsTable(table_name)
  )
  
  return True
