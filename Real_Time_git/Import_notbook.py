# Databricks notebook source
# MAGIC %run "./Class"

# COMMAND ----------

df_new = spark.read.format('csv')\
    .option("header",True)\
    .option("inferSchema",True)\
    .load('/FileStore/RawSource/fifa_case_study.csv')

# COMMAND ----------

obj = Window_Function()

# COMMAND ----------

obj.df = df_new

# COMMAND ----------

new_r_n = obj.Row_Number('r_n','Name')

# COMMAND ----------

display(new_r_n)
