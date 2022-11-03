-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## create dimDate table
-- MAGIC 
-- MAGIC based on example https://raw.githubusercontent.com/BlueGranite/calendar-dimension-spark/main/CalendarDimension.sql
-- MAGIC and https://github.com/MarcosCarnevale/D_Calendar_Pyspark/blob/main/d_calendar.py

-- COMMAND ----------

-- DBTITLE 1,Generate Raw Dates
-- MAGIC %python
-- MAGIC from pyspark.sql.functions import explode, sequence, to_date
-- MAGIC 
-- MAGIC df = spark.sql("SELECT min (to_date(start_at)) as beginDate, add_months(max (to_date(ended_at)),12) as endDate FROM silver_trips")
-- MAGIC 
-- MAGIC beginDate = df.first()["beginDate"]
-- MAGIC endDate = df.first()["endDate"]
-- MAGIC 
-- MAGIC spark.sql(f"select explode(sequence(to_timestamp('{beginDate}'), to_timestamp('{endDate}'), interval 1 day)) as calendarDate") \
-- MAGIC     .createOrReplaceTempView('dates')

-- COMMAND ----------

-- DBTITLE 1,Examine dates temporary view
select * from dates limit 5

-- COMMAND ----------

-- DBTITLE 1,Save as Delta Table
-- use the query developed above to load the calendar dimension into a Delta Lake table
create or replace table dimDate
using delta
location 'delta'
as select
  year(calendarDate) * 10000 + month(calendarDate) * 100 + day(calendarDate) as DateInt,
  CalendarDate,
  year(calendarDate) AS CalendarYear,
  date_format(calendarDate, 'MMMM') as CalendarMonth,
  month(calendarDate) as MonthOfYear,
  date_format(calendarDate, 'EEEE') as CalendarDay,
  dayofweek(calendarDate) as DayOfWeek,
  weekday(calendarDate) + 1 as DayOfWeekStartMonday,
  case
    when weekday(calendarDate) < 5 then 'Y'
    else 'N'
  end as IsWeekDay,
  dayofmonth(calendarDate) as DayOfMonth,
  case
    when calendarDate = last_day(calendarDate) then 'Y'
    else 'N'
  end as IsLastDayOfMonth,
  dayofyear(calendarDate) as DayOfYear,
  weekofyear(calendarDate) as WeekOfYearIso,
  quarter(calendarDate) as QuarterOfYear,
  /* Use fiscal periods needed by organization fiscal calendar */
  case
    when month(calendarDate) >= 10 then year(calendarDate) + 1
    else year(calendarDate)
  end as FiscalYearOctToSep,
  (month(calendarDate) + 2) % 12 + 1 as FiscalMonthOctToSep,
  case
    when month(calendarDate) >= 7 then year(calendarDate) + 1
    else year(calendarDate)
  end as FiscalYearJulToJun,
  (month(calendarDate) + 5) % 12 + 1 as FiscalMonthJulToJun
from
  dates

-- COMMAND ----------

-- DBTITLE 1,Optimize
optimize dimDate zorder by (calendarDate)

-- COMMAND ----------

-- DBTITLE 1,Examine the Calendar Dimension
select * from dimDate limit 5

-- COMMAND ----------


