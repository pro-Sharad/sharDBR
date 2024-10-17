# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

input_path = "/Volumes/sharad_dev/default/raw/"

# COMMAND ----------

def add_ingestion(df):
    df1 = df.withColumn("ingestion_date",current_timestamp())
    return df1

# COMMAND ----------


