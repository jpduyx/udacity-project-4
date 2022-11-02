# Databricks notebook source
# DBTITLE 1,load into silver tables
# MAGIC %md
# MAGIC create tables and loads data from Delta files. using spark.sql statements to create the tables and then load data from the files that were extracted in the Extract step.

# COMMAND ----------

# DBTITLE 1,riders
bronze = "/delta/bronze_riders"
silver = "silver_riders"
create_table = "CREATE TABLE %s USING DELTA LOCATION '%s'" % (silver, bronze)
spark.sql(create_table)

# COMMAND ----------

# DBTITLE 1,stations
bronze = "/delta/bronze_stations"
silver = "silver_stations"

create_table = "CREATE TABLE %s USING DELTA LOCATION '%s'" % (silver, bronze)
spark.sql(create_table)

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


