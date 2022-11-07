# Databricks notebook source
# MAGIC %md
# MAGIC ## dfbf utils
# MAGIC copy, move, delete
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC Warning
# MAGIC 
# MAGIC The Python implementation of all * dbutils.fs * methods uses snake_case rather than camelCase for keyword formatting.
# MAGIC 
# MAGIC For example: while dbuitls.fs.help() displays the option extraConfigs for dbutils.fs.mount(), in Python you would use the keywork extra_configs.

# COMMAND ----------

dbutils.fs.head("/tmp/my_file.txt", 25)
# [Truncated to first 25 bytes]
dbutils.help()

dbutils.fs.help()

dbutils.fs.help("cp")



# COMMAND ----------

# summarize data 
df = spark.read.format('csv').load(
  '/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv',
  header=True,
  inferSchema=True
)
dbutils.data.summarize(df)

# COMMAND ----------

# remove all dim tables 
dbutils.fs.rm("/delta/dimDate", recurse=True)
dbutils.fs.rm("/delta/dimStation", recurse=True)
dbutils.fs.rm("/delta/dimRider", recurse=True)


# COMMAND ----------

# remove all fact tables
dbutils.fs.rm("/delta/factPayment", recurse=True)
dbutils.fs.rm("/delta/factTrip", recurse=True)
