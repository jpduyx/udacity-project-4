# udacity-project-4 Azure Data Bricks

 [v] star schema star-erd.pdf
 * bronze datastore created in azure databricks with Delta Lake
 * gold datastore created in Delta Lake Tables
 * transform the data into star schema for the Gold data store
 

## Business outcomes 
 
 1. Analyze how much time spend per ride, based on
   
   * date and time factors such as day of week and time of day 
   * which station is starting and / or ending station
   * age of the rider at time of the ride
   * whether the rider is a (paying) member or casual rider
   
 2. Analyze how much money is spend: 

  * per month, quarter, year 
  * per member, based on the age of the rider at account start
  

## Deliverables: 

 [v] Star Schema
   [v] 2 fact tables related to payment and trip information
   [v] dimensions related to the trip data: riders, stations and date
   [v] dimensions related to the payment data: dates and riders
   
 * Extract
  * python code in jupyter notebooks and scripts to extract information from CSV files stored in Databricks and write it to the Delta Filesystem
 !!??  * python code that picks up files from the Databricks file system storage and writes it out to Delta file location
  
 * Load
  * implement key features of data lakes on Azure: python code in notebook that contains code to create tables and loads data from Delta files. using spark.sql statements to create the tables and then load data from the files that were extracted in the Extract step.
  
  
 * Transform 
  * 
  * 
  
   
 
  
 
## TODO

 
## NOTE 

As far as I can see now, it looks like the star schema is same as the star schema for project-3 so reusing that for now

