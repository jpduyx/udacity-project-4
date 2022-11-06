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
# MAGIC The transform scripts should at minimum adhere to the following: should write to delta; should use overwrite mode; **save as a table in delta**.
# MAGIC 
# MAGIC ## BUSINESS OUTCOMES
# MAGIC 
# MAGIC 1. Analyze how much time spend per ride, based on
# MAGIC    
# MAGIC    * [ ] date and time factors such as day of week and time of day 
# MAGIC    * which station is starting and / or ending station
# MAGIC    * age of the rider at time of the ride
# MAGIC    * whether the rider is a (paying) member or casual rider
# MAGIC    
# MAGIC  2. Analyze how much money is spend: 
# MAGIC 
# MAGIC   * per month, quarter, year 
# MAGIC   * per member, based on the age of the rider at account start

# COMMAND ----------

# DBTITLE 1,dimRider
silver = "silver.riders"
gold = "dimRider"
#create = "CREATE TABLE %s (id INT, firs VARCHAR(40), last VARCHAR (40), birthday DATE, account_start DATE, account_end DATE, is_member BOOLEAN )" % (gold)
#spark.sql(create)
silverdf = spark.sql(f"SELECT * FROM {silver}")
silverdf
silverdf.show()
df = silverdf
df.write.mode("overwrite").save(f"/delta/{gold}")

# COMMAND ----------

# DBTITLE 1,dimStation
silver = "silver.stations"
gold = "dimStation"

#create = "CREATE TABLE %s (id VARCHAR(50), name VARCHAR(150), \
#                           latitude FLOAT, longitude FLOAT )" % (gold)

silverdf = spark.sql(f"SELECT * FROM {silver}")
silverdf.show()
df = silverdf
df.write.mode("overwrite").save(f"/delta/{gold}")


# COMMAND ----------

# DBTITLE 1,dimDate
from pyspark.sql.functions import explode, sequence, to_date
from dateutil.relativedelta import relativedelta
import pyspark.sql.functions as F

silver = "silver.trips"

(beginDate, endDate) = spark.sql(f"SELECT min (to_date(start_at)) as beginDate, add_months(max (to_date(ended_at)),12) as endDate FROM {silver}").first()
endDate = endDate + relativedelta(months=24)

spark.sql(f"select explode(sequence(to_timestamp('{beginDate}'), (to_timestamp('{endDate}')) , interval 1 hour)) as ts") \
    .createOrReplaceTempView('dates')

create = """
create or replace table dimDate
USING delta
LOCATION '/delta/dimDate'
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
silver = "silver.payments"
gold = "factPayment"

#create = "CREATE TABLE %s (id INT, date DATE, amount FLOAT, rider INT)" % (gold)
#spark.sql(create)

silverdf = spark.sql(f"SELECT * FROM {silver}")
silverdf.show()
df = silverdf
df.write.mode("overwrite").save(f"/delta/{gold}")


# COMMAND ----------

# DBTITLE 1,factTrip
import pyspark.sql.functions as F

silver = "silver.trips"
rider = "silver.riders"
gold = "factTrip"

joineddf = spark.sql(f"""SELECT t.id, t.start_at, t.ended_at, t.duration, t.start_station, t.dest_station, t.rideable_type, t.rider_id, 
                                CAST (datediff (year, r.birthday, t.start_at) AS INTEGER) as rider_age 
                         FROM {silver} as t
                         LEFT JOIN {rider} as r ON t.rider_id = r.id
                      """)
df = joineddf.withColumn("duration",(F.col("ended_at").cast("int") - F.col("start_at").cast("int")))
df
df.show(5, truncate=False)
df.write.mode("overwrite").save(f"/delta/{gold}")
