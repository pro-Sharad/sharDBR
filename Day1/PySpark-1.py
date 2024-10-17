# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

print("hi")

# COMMAND ----------

data = [(1,'a',20),(2,'b',30)]
schema = "id int, name string, age int"
df = spark.createDataFrame(data,schema)
display(df)

# COMMAND ----------

df.select(col("id").alias("emp_id")).display()

# COMMAND ----------

df.withColumnRenamed(col("name"),col("gud_name")).display()

# COMMAND ----------

df.withColumnsRenamed({"name":"gud_name","id":"emp_id"}).display()

# COMMAND ----------

df.withColumn("current_date",current_date()).display()

# COMMAND ----------


