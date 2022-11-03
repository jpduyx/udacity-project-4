# Databricks notebook source
# MAGIC %md
# MAGIC # Extract to Delta FS
# MAGIC 
# MAGIC Read the uploaded CSV files from DBFS into a dataframe and then write them to a Delta FS fles. 

# COMMAND ----------

#dbutils.fs.rm('/delta/bronze_trips',recurse=True)
#dbutils.fs.rm('/user/hive',recurse=True)

# COMMAND ----------

# DBTITLE 1,riders
filename = "dbfs:/FileStore/tables/riders.csv"
df = spark.read.csv(filename).toDF("id","first","last", "address", "birthday", "account_start", "account_end", "is_member")
bronze = df.selectExpr("cast(id as int) id", "cast(first as varchar(40)) first", "cast (last as varchar(40)) last", \
                       "cast(address as varchar(40)) address", "cast(birthday as date) birthday", \
                       "cast(account_start as date) account_start", "cast(account_end as date) account_end", \
                       "cast(is_member as boolean) is_member")
bronze.printSchema()
bronze.show(5)

bronze.write.format("delta").mode("overwrite").save("/delta/bronze_riders")


# COMMAND ----------

# DBTITLE 1,stations
filename = "/FileStore/tables/stations.csv"
df = spark.read.csv(filename).toDF("id","name","latitude", "longitude")
bronze = df.selectExpr("cast(id as varchar(40)) id", "cast(name as varchar(50)) name", "cast (latitude as float) latitude", "cast (longitude as float) longitude")
display(bronze)
bronze.write.format("delta").mode("overwrite").save("/delta/bronze_stations")

# COMMAND ----------

# DBTITLE 1,payments
filename = "/FileStore/tables/payments.csv"
df = spark.read.csv(filename).toDF("id","date","amount", "rider_id")
bronze = df.selectExpr("cast(id as int)", "cast(date as date)", "cast (amount as float)", "cast (rider_id as int)")
#display(bronze)
bronze.write.format("delta").mode("overwrite").save("/delta/bronze_payments")

# COMMAND ----------

# DBTITLE 1,trips
filename = "/FileStore/tables/trips.csv"
df = spark.read.csv(filename).toDF("id","rideable_type","start_at", "ended_at", "start_station", "dest_station", "user_id")
bronze = df.selectExpr("cast(id as varchar(40))", "cast(rideable_type as varchar(15))", "cast (start_at as timestamp)", "cast (ended_at as timestamp)", \
                       "cast (start_station as varchar(40))", "cast(dest_station as varchar(40))", "cast(user_id as int) rider_id" )
display(bronze)
bronze.write.format("delta").mode("overwrite").save("/delta/bronze_trips")
