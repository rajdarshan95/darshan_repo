# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

df = spark.read.format("csv")\
    .option("header",True)\
    .option("inferschema",True)\
    .load('/FileStore/RawSource/sales_data_first.csv')
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Manually repartationing because the data is not more then 128MB**

# COMMAND ----------

df = df.repartition(4)

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **To check the partation**

# COMMAND ----------

df = df.withColumn('partation_id',spark_partition_id())

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Groping the partation and counting the number of records in it**

# COMMAND ----------

df = df.withColumn('partation_id',spark_partition_id()).groupBy('partation_id').count()

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Giving the rank based on the count of records**

# COMMAND ----------

df = df.withColumn('Rank',dense_rank().over(Window.orderBy('count')))

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Creating a GlobalView

# COMMAND ----------

df = df.createOrReplaceGlobalTempView("global_view")

# COMMAND ----------

# MAGIC %md
# MAGIC **Quering the DataFrame**

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.global_view
