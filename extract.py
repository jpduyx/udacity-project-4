# Databricks notebook source
# MAGIC %md
# MAGIC # Extract to Delta FS
# MAGIC 
# MAGIC Read the uploaded CSV files from DBFS into a dataframe and then write them to a Delta FS fles. 

# COMMAND ----------

# DBTITLE 1,riders
filename = "dbfs:/FileStore/tables/riders.csv"
df = spark.read.csv(filename).toDF("id","first","last", "address", "birthday", "account_start", "account_end", "is_member")
display(df)
df.write.format("delta").mode("overwrite").save("/delta/bronze_riders")

# COMMAND ----------

# DBTITLE 1,stations
filename = "/FileStore/tables/stations.csv"
df = spark.read.csv(filename).toDF("id","name","latitude", "longitude")
display(df)
df.write.format("delta").mode("overwrite").save("/delta/bronze_stations")

# COMMAND ----------

# DBTITLE 1,payments
filename = "/FileStore/tables/payments.csv"
df = spark.read.csv(filename).toDF("id","date","amount", "rider_id")
display(df)
df.write.format("delta").mode("overwrite").save("/delta/bronze_payments")

# COMMAND ----------

# DBTITLE 1,trips
filename = "/FileStore/tables/trips.csv"
df = spark.read.csv(filename).toDF("id","rideable_type","start_at", "ended_at", "start_station", "dest_station", "user_id")

display(df)
df.write.format("delta").mode("overwrite").save("/delta/bronze_trips")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC I checked that All data is exported to Databricks Delta files
