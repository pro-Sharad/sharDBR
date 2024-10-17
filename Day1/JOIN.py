# Databricks notebook source
df_sales = spark.table("sales_table")

# COMMAND ----------

df_customer = spark.table("customers_table")

# COMMAND ----------

df_products = spark.table("products_table")

# COMMAND ----------

df_join = df_sales.join(df_customer,"customer_id","inner")

# COMMAND ----------

display(df_customer)

# COMMAND ----------

df_customer.where("customer_id = 1").display()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df_customer.sort(col("customer_city").desc()).display()

# COMMAND ----------

df_customer.groupBy("customer_id").count().orderBy("customer_id").display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customers_table c
# MAGIC join 
# MAGIC sales s
# MAGIC on c.customer_id = s.customer_id 
# MAGIC where s.customer_id = 5

# COMMAND ----------


