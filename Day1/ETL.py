# Databricks notebook source
# MAGIC %run
# MAGIC /Workspace/Users/sharadbj9655@gmail.com/Day1/Includes

# COMMAND ----------

df_orders = spark.read.csv(f"{input_path}order_dates.csv",header=True,inferSchema=True)

# COMMAND ----------

df1 = add_ingestion(df_orders)

# COMMAND ----------

drop()

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table sales

# COMMAND ----------

df1.write.mode("overwrite").saveAsTable("orders")

# COMMAND ----------


