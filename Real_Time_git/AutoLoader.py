# Databricks notebook source
dbutils.fs.ls('/FileStore/RawSource')

# COMMAND ----------

# MAGIC %md
# MAGIC **remove any file/folder**

# COMMAND ----------

dbutils.fs.rm('/FileStore/Rawdestination',True)

# COMMAND ----------

# MAGIC %md
# MAGIC **With same Schema and With different schema**

# COMMAND ----------

df = spark.read.format("csv")\
    .option("header",True)\
    .option("inferschema",True)\
    .load('/FileStore/RawSource/fifa_case_study.csv')
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Incremantaly read the data by Autoloader**

# COMMAND ----------

df = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .option("cloudFiles.schemaLocation", "/FileStore/Rawdestination/Checkpoint") \
    .load("/FileStore/RawSource")

# COMMAND ----------

# MAGIC %md
# MAGIC **Incremantaly Write the data by Autoloader**

# COMMAND ----------

df.writeStream.format("delta")\
    .option("checkpointLocation","/FileStore/Rawdestination/data")\
    .trigger(processingTime="3 second")\
    .option('margeSchema',True)\
    .start("/FileStore/Rawdestination/data")

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading the delta file**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from delta.`/FileStore/Rawdestination/data`
