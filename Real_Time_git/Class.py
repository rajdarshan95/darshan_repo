# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

df = spark.read.format('csv')\
    .option("header",True)\
    .option("inferschema",True)\
    .load("/FileStore/RawSource/fifa_case_study.csv")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Creating class

# COMMAND ----------

class Window_Function:
    def __init__(self):
        self.df = spark.read.format('csv')\
            .option("header", True)\
            .option("inferSchema", True)\
            .load("/FileStore/RawSource/fifa_case_study.csv")

    def Dense_Rank(self, new_col, part_col, ord_col):
        self.df = self.df.withColumn(new_col,dense_rank().over(Window.partitionBy(part_col).orderBy(col(ord_col).desc())))
        return self.df
        
    def Rank(self, new_col, part_col, ord_col):
        self.df = self.df.withColumn(new_col,rank().over(Window.partitionBy(part_col).orderBy(col(ord_col).desc())))
        return self.df
    
    def Row_Number(self, new_col,ord_col):
        self.df = self.df.withColumn(new_col,row_number().over(Window.orderBy(col(ord_col).desc())))
        return self.df

# COMMAND ----------

# MAGIC %md
# MAGIC **Creating Class object**

# COMMAND ----------

obj = Window_Function()

# COMMAND ----------

# MAGIC %md
# MAGIC **Method DenseRank**

# COMMAND ----------

df_dense = obj.Dense_Rank('NumberOfPlayers','Nationality','LongPassing')

# COMMAND ----------

df_dense.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Method Rank**

# COMMAND ----------

df_rank = obj.Rank('NumberOfPlayers','Nationality','LongPassing')

# COMMAND ----------

display(df_rank)

# COMMAND ----------

# MAGIC %md
# MAGIC **Method Row Number**

# COMMAND ----------

df_r_n = obj.Row_Number('Row_number','Name')

# COMMAND ----------

display(df_r_n)
