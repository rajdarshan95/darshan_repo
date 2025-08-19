# Databricks notebook source
# MAGIC %md
# MAGIC **Slowly changing intial and incremental**

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable

# COMMAND ----------

dbutils.fs.ls("/FileStore/raw_csv")

# COMMAND ----------

df = spark.read.format("csv")\
    .option("inferSchema",True)\
    .option("header",True)\
    .load("/FileStore/raw_csv/products_dim_table.csv")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Basic transformation to remove null values**

# COMMAND ----------

df = df.dropna('all')

# COMMAND ----------

df = df.select('p_id','p_name','p_category')

# COMMAND ----------

# MAGIC %md
# MAGIC **Creating the variable**

# COMMAND ----------

initial_run = 0

# COMMAND ----------

# MAGIC %md
# MAGIC ### Main Code

# COMMAND ----------

if initial_run == 0:
    delta_table = DeltaTable.forPath(spark, "/FileStore/rawcsvsink")
    delta_table.alias("tag").merge(
        df.alias("src"),
        "tag.p_id = src.p_id"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()

else:
    df.write.format("delta") \
        .mode("append") \
        .option("path", "/FileStore/rawcsvsink") \
        .saveAsTable("productDim")


# COMMAND ----------

# MAGIC %md
# MAGIC **looking the delta tables**

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from productDim
