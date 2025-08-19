# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df = spark.read.format("csv")\
    .option("header",True)\
    .option("inferschema",True)\
    .load('/FileStore/RawSource/fifa_case_study.csv')
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Condational column based on the "Age"**

# COMMAND ----------

df = df.withColumn('age_range',when(col('Age') < 25,'fit')\
  .when(col('Age') < 30,'little_fit')\
  .otherwise('unfit'))

# COMMAND ----------

display(df)
