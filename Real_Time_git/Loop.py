# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df = spark.read.format("csv")\
    .option("header",True)\
    .option("inferschema",True)\
    .load('/FileStore/RawSource/sales_data_first.csv')
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Loop

# COMMAND ----------

var_unit_sold = [1,2,3]

# COMMAND ----------

for i in var_unit_sold:
    df = spark.read.format("csv")\
    .option("header",True)\
    .option("inferschema",True)\
    .load('/FileStore/RawSource/sales_data_first.csv')\
    .filter(col('Units_Sold')== i)

    df.write.format('csv')\
        .mode('append')\
        .option('path',f'/FileStore/loopdata/unitsold = {i}')\
        .save()
