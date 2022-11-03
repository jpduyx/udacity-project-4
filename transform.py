# Databricks notebook source
# DBTITLE 1,transform into gold tables
# MAGIC %md
# MAGIC **1. Use Spark and Databricks to run ELT processes by creating fact tables** | 
# MAGIC The fact table Python scripts should contain appropriate keys from the dimensions. In addition, the fact table scripts should appropriately generate the correct facts based on the diagrams provided in the first step. 
# MAGIC 
# MAGIC **2. Use Spark and Databricks to run ELT processes by creating dimension tables** | 
# MAGIC The dimension Python scripts should match the schema diagram. Dimensions should generate appropriate keys and should not contain facts. 
# MAGIC 
# MAGIC **3. Produce Spark code in Databricks using Jupyter Notebooks and Python scripts** | 
# MAGIC The transform scripts should at minimum adhere to the following: should write to delta; should use overwrite mode; save as a table in delta. 

# COMMAND ----------

# DBTITLE 1,dimRider
silver = "silver_riders"
gold = "dimRider"
create = "CREATE TABLE %s (id INT, firs VARCHAR(40), last VARCHAR (40), birthday DATE, \
                           account_start DATE, account_end DATE, is_member BOOL )" % (gold)
spark.sql(create)


# COMMAND ----------

# DBTITLE 1,dimStation
silver = "silver_stations"
gold = "dimStation"

create = "CREATE TABLE %s (id VARCHAR(40), name VARCHAR(50), \
                           latitude FLOAT, longitude FLOAT )" % (gold)
spark.sql(create)

# COMMAND ----------

# DBTITLE 1,dimDate
from pyspark.sql.functions import explode, sequence, to_date

df = spark.sql("SELECT min (to_date(start_at)) as beginDate, add_months(max (to_date(ended_at)),12) as endDate FROM silver_trips")

beginDate = df.first()["beginDate"]
endDate = df.first()["endDate"]

spark.sql(f"select explode(sequence(to_timestamp('{beginDate}'), to_timestamp('{endDate}'), interval 1 hour)) as ts") \
    .createOrReplaceTempView('dates')

create = """
create or replace table dimDate
as select
  ts,
  hour(ts) AS hour,
  dayofweek(ts) as dayofweek,
  dayofmonth(ts) as dayofmonth,
  weekofyear(ts) as weekofyear,
  month(ts) as month,
  quarter(ts) as quarter,
  year(ts) AS year
from
  dates
"""
spark.sql(create)
spark.sql("optimize dimDate zorder by (ts)")

# COMMAND ----------

# DBTITLE 1,factPayment
silver = "silver_payments"
gold = "factPayment"

create = "CREATE TABLE %s (id INT, firs VARCHAR(40), last VARCHAR (40), birthday DATE, \
                           account_start DATE, account_end DATE, is_member BOOL )" % (gold)
spark.sql(create)


# COMMAND ----------

# DBTITLE 1,factTrip
silver = "silver_trips"
gold = "factTrip"

create = "CREATE TABLE %s (id INT, firs VARCHAR(40), last VARCHAR (40), birthday DATE, \
                           account_start DATE, account_end DATE, is_member BOOL )" % (gold)
spark.sql(create)
