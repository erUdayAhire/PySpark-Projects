# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

spark=SparkSession.builder.appName("SET OPERATION").getOrCreate()

# COMMAND ----------

master_df=spark.read.option("delimiter","|").csv("/FileStore/tables/total_stud_list.csv",header=True)
daily_df=spark.read.option("delimiter","|").csv("/FileStore/tables/today_stud_list.csv",header=True) 

# COMMAND ----------

master_df.show()
daily_df.show()

# COMMAND ----------

master_df.intersect(daily_df).show()

# COMMAND ----------

master_df.intersectAll(daily_df).show()

# COMMAND ----------

master_df.join(daily_df,on=["Roll_no"],how="leftsemi").show()

# COMMAND ----------

master_df.subtract(daily_df).show()

# COMMAND ----------

master_df.exceptAll(daily_df).show()

# COMMAND ----------

master_df.join(daily_df,on=["Roll_no"],how="leftanti").show()

# COMMAND ----------


