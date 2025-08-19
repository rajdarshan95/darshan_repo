# Databricks notebook source
# MAGIC %md
# MAGIC ###Creating Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC create table deltasource
# MAGIC (
# MAGIC   id int,
# MAGIC   name string,
# MAGIC   salary int
# MAGIC )
# MAGIC using delta
# MAGIC location '/FileStore/deltasource/source1'

# COMMAND ----------

# MAGIC %md
# MAGIC ###Turning off the deletion vector

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE deltasource SET TBLPROPERTIES ('delta.enableDeletionVectors' = false);

# COMMAND ----------

# MAGIC %md
# MAGIC ###inserting values into to the delta table

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into deltasource
# MAGIC values
# MAGIC (1,'ravi',100),
# MAGIC (2,'ramu',200),
# MAGIC (3,'shamu',300)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Reading the data from the delta table through streaming

# COMMAND ----------

df = spark.readStream.table('deltasource')

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading the data from the delta table through streaming from perticular version**

# COMMAND ----------

# df_new = spark.readStream.option('StartingVersion',7).table('deltasource')

df.writeStream.format('delta')\
    .option('CheckPointLocation','/FileStore/deltasource/sink1/checkpoint')\
    .option('path','/FileStore/deltasource/sink1/data')\
    .trigger(processingTime= '3 second')\
    .start()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Writing the data to the destination through stream

# COMMAND ----------

df.writeStream.format('delta')\
    .option('CheckPointLocation','/FileStore/deltasource/sink1/checkpoint')\
    .option('path','/FileStore/deltasource/sink1/data')\
    .trigger(processingTime= '3 second')\
    .start()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Versioning

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY deltasource

# COMMAND ----------

# MAGIC %md
# MAGIC ###Viewing sink1/data deltatable

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`/FileStore/deltasource/sink1/data`

# COMMAND ----------

# MAGIC %md
# MAGIC ###Viewing sink1/data1 deltatable

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`/FileStore/deltasource/sink1/data1`
