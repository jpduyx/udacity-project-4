# Databricks notebook source
# DBTITLE 1,load into silver tables
# MAGIC %md
# MAGIC create tables and loads data from Delta files. using spark.sql statements to create the tables and then load data from the files that were extracted in the Extract step.

# COMMAND ----------

warehousedir = "delta" 
spark.catalog.listDatabases()

# COMMAND ----------

spark.sql("create database if not exists silver location '%s'" % (warehousedir))

# COMMAND ----------

# DBTITLE 1,riders

bronze = "/%s/bronze_riders" % (warehousedir)
silver = "silver.riders"
drop = "drop table if exists %s" % (silver) 
create = "CREATE TABLE %s (id INT, first VARCHAR(40), last VARCHAR (40), birthday DATE, address VARCHAR(40), \
                           account_start DATE, account_end DATE, is_member BOOLEAN)" % (silver)
copy = """COPY INTO %s FROM '%s' FILEFORMAT = %s COPY_OPTIONS ('mergeSchema' = 'true') """ % (silver, bronze, "DELTA")

spark.sql(drop)
spark.sql(create)
spark.sql("describe table silver.riders").show()
spark.sql(copy)
spark.sql("describe table silver.riders").show()
#spark.sql("describe detail /delta/bronze_riders").show()
spark.sql("select * from silver.riders limit 25").show(25)


# COMMAND ----------

# DBTITLE 1,stations
bronze = "/delta/bronze_stations"
silver = "silver.stations"
drop = "drop table if exists %s" % (silver) 
create = "CREATE TABLE %s (id VARCHAR(50), name VARCHAR(150), \
                           latitude FLOAT, longitude FLOAT)" % (silver)

copy = """COPY INTO %s FROM '%s' FILEFORMAT = %s COPY_OPTIONS ('mergeSchema' = 'true') """ % (silver, bronze, "DELTA")
spark.sql(drop)
spark.sql(create)
spark.sql(copy)
spark.sql("describe table %s" %(silver) ).show()
spark.sql("select * from %s limit 25" %(silver)).show()

# COMMAND ----------

# DBTITLE 1,payments
bronze = "/delta/bronze_payments"
silver = "silver.payments"
drop = "drop table if exists %s" % (silver) 

create = "CREATE TABLE %s (id INT, date DATE, \
                           amount FLOAT, rider_id INT)" % (silver)
copy = """COPY INTO %s FROM '%s' FILEFORMAT = %s COPY_OPTIONS ('mergeSchema' = 'true') """ % (silver, bronze, "DELTA")
spark.sql(drop)
spark.sql(create)
spark.sql(copy)
spark.sql("describe table %s" %(silver) ).show()
spark.sql("select * from %s limit 25" %(silver)).show()

# COMMAND ----------

# DBTITLE 1,trips
bronze = "/delta/bronze_trips"
silver = "silver.trips"
drop = "drop table if exists %s" % (silver) 
create = "CREATE TABLE %s (id VARCHAR(50), start_at TIMESTAMP, ended_at TIMESTAMP, \
                           duration INT, start_station VARCHAR(50), dest_station VARCHAR(50), \
                           rideable_type VARCHAR(15), rider_id INT, rider_age INT, is_member BOOLEAN \
                           )" % (silver)

copy = """COPY INTO %s FROM '%s' FILEFORMAT = %s COPY_OPTIONS ('mergeSchema' = 'true') """ % (silver, bronze, "DELTA")
spark.sql(drop)
spark.sql(create)
spark.sql(copy)
spark.sql("describe table %s" %(silver) ).show()
spark.sql("select * from %s limit 25" %(silver)).show()

# COMMAND ----------


