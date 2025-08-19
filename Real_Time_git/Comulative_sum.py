# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

df = spark.read.format("csv")\
    .option("header",True)\
    .option("inferschema",True)\
    .load('/FileStore/RawSource/fifa_case_study.csv')
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Creating a cumulative sum on unit sold column**

# COMMAND ----------

df = df.withColumn('cum_sum',sum('_c0').over(Window.partitionBy('Club').orderBy('Club').rowsBetween(Window.unboundedPreceding,Window.currentRow)))

# COMMAND ----------

display(df)
