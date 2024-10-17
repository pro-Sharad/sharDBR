# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

from pyspark.sql import Row
import datetime

# COMMAND ----------

users=[
    {
        "id":1,
        "name":"Sachin",
        "last_name":"Tendulkar",
        "email":"sachin@gmail.com",
        "Mobile":Row(mobile= "342779900", home= "91568557700"),
        "courses": [1,2],
        "is_customer":True,
        "DOB": datetime.date(1973,4,24)
    },
    {
         "id":2,
        "name":"Virat",
        "last_name":"Kohli",
        "email":"virat@gmail.com",
        "Mobile":Row(mobile= "91556600", home= "918912300"),
        "courses": [2,3],
        "is_customer":True, 
        "DOB":datetime.date(1988,11,5)
    },
     {
         "id":3,
        "name":"Rohit",
        "last_name":"Sharma",
        "email":"rohit@gmail.com",
        "Mobile":Row(mobile= "914455700", home= "9145997700"),
        "courses": [3],
        "is_customer":True, 
        "DOB":datetime.date(1987,4,30)
     },
     {
         "id":4,
        "name":"Dinesh",
        "last_name":"Karthik",
        "email":"dinesh@gmail.com",
        "Mobile":Row(mobile= "91467700", home= "916789700"),
        "courses": [3],
        "is_customer":True, 
        "DOB":datetime.date(1985,6,1)
     },
     {
         "id":5,
        "name":"M S",
        "last_name":"Dhoni",
        "email":"dhoni@gmail.com",
        "Mobile":Row(mobile= "91467799", home= "916778800"),
        "courses": [3],
        "is_customer":True, 
        "DOB":datetime.date(1981,7,7)
     }
]

# COMMAND ----------

df = spark.createDataFrame(data=users)

# COMMAND ----------

df_final = df.withColumnRenamed("id","emp_id") \
    .withColumn("ingestion_date",current_timestamp())\
        .withColumn("full_name",concat("name",lit(" "),"last_name"))\
            .drop("is_customer","name","last_name")

# COMMAND ----------

display(df_final)

# COMMAND ----------

df_final.columns

# COMMAND ----------

df_users_final = df_final.withColumn("office_mobile",col("Mobile.mobile")) \
.withColumn("home_mobile",col("Mobile.home")) \
    .withColumn("courses",explode("courses")) \
.drop("Mobile")

# COMMAND ----------


