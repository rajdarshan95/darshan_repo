# Databricks notebook source
try:
    df = spark.read.format("csv")\
        .option("header",True)\
        .option("inferschema",True)\
        .load('/FileStore/RawSource/first.csv')
    display(df)
except Exception as e:
    print(f"the error is --> {e}")

# COMMAND ----------

print('hello world')
