# Databricks notebook source
# DBTITLE 1,load into silver tables
# MAGIC %md
# MAGIC create tables and loads data from Delta files. using spark.sql statements to create the tables and then load data from the files that were extracted in the Extract step.

# COMMAND ----------

# DBTITLE 1,riders
bronze = "/delta/bronze_riders"
silver = "silver_riders"
create = "CREATE TABLE %s (id INT, firs VARCHAR(40), last VARCHAR (40), birthday DATE, \
                           account_start DATE, account_end DATE, is_member BOOL)" % (silver)
###insert = "CREATE TABLE %s USING DELTA LOCATION '%s'" % (silver, bronze)

spark.sql(create)
spark.sql(insert)

# COMMAND ----------

# DBTITLE 1,stations
bronze = "/delta/bronze_stations"
silver = "silver_stations"
create = "CREATE TABLE %s (station_id VARCHAR(40), name VARCHAR(50), \
                           latitude FLOAT, longitude FLOAT)" % (silver)

copy = "COPY INTO %s FROM '%s' FILEFORMAT = %s" % (silver, bronze, "delta")
spark.sql("COPY INTO " + table_name + \
  " FROM '" + source_data + "'" + \
  " FILEFORMAT = " + source_format
)
spark.sql(create)
spark.sql(copy)

# COMMAND ----------

# DBTITLE 1,payments
bronze = "/delta/bronze_payments"
silver = "silver_payments"

create_table = "CREATE TABLE %s USING DELTA LOCATION '%s'" % (silver, bronze)
spark.sql(create_table)


# COMMAND ----------

# DBTITLE 1,trips
bronze = "/delta/bronze_trips"
silver = "silver_trips"

create_table = "CREATE TABLE %s USING DELTA LOCATION '%s'" % (silver, bronze)
spark.sql(create_table)


# COMMAND ----------


