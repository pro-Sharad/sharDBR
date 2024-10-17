# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC create table sales_table as
# MAGIC select * from csv.`/Volumes/sharad_dev/default/raw/sales.csv`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sales_table

# COMMAND ----------

# MAGIC %sql
# MAGIC create table customers_table as
# MAGIC select *, current_timestamp() as ingstion_date from json.`/Volumes/sharad_dev/default/raw/customers.json`

# COMMAND ----------

# MAGIC %sql
# MAGIC create table products_table as
# MAGIC select *, current_timestamp() as ingstion_date from json.`/Volumes/sharad_dev/default/raw/products.json`

# COMMAND ----------

# MAGIC %sql
# MAGIC create table orderdates_table as
# MAGIC select *, current_timestamp() as ingstion_date from csv.`/Volumes/sharad_dev/default/raw/order_dates.csv`

# COMMAND ----------


