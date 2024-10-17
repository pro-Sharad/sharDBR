# Databricks notebook source


# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df = spark.read.csv("/Volumes/sharad_dev/default/raw/sales.csv",header=True,inferSchema=True)

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("sales")

# COMMAND ----------

display(df)

# COMMAND ----------

df_json = spark.read.json("/Volumes/sharad_dev/default/raw/customers.json")

# COMMAND ----------

display(df_json)

# COMMAND ----------

df_json.write.saveAsTable("customers")

# COMMAND ----------


