# Databricks notebook source
# MAGIC %md
# MAGIC # Mortgage Loan Data Analysis Performance 
# MAGIC 
# MAGIC <p></p>
# MAGIC <img src='https://www.corelogic.com/wp-content/uploads/sites/4/2021/05/Loan-Performance-Insights-e1639430812246.jpg' width="800">

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##Building a Next Generation Query Engine
# MAGIC - Re-architected for the fastest performance on real-world applications 
# MAGIC   - Native C++ engine for faster queries
# MAGIC   - Custom built memory management to avoid JVM bottlenecks
# MAGIC   - Vectorized: memory, instruction, and data parallelism (SIMD)
# MAGIC - Works with your existing code and **avoids vendor lock-in**
# MAGIC   - 100% compatible with open source Spark DataFrame APIs and Spark SQL
# MAGIC   - Transparent operation to users - no need to invoke something new, it just works
# MAGIC - Optimizing for all data use cases and workloads
# MAGIC   - Today, supporting SQL and DataFrame workloads
# MAGIC   - Coming soon, Streaming, Data Science, and more

# COMMAND ----------

# MAGIC %md
# MAGIC ## Use Cases
# MAGIC -  **Where Photon Helps** - Photon demonstrates the largest benefits for longer running jobs/queries on large data sets (10s of millions of rows).  Since Photon only impacts the execution phase of the job (vs planning, compilation, scheduling, IO, etc).  The best impact is on workloads with a high volume of batch data, with calculations, aggregations, and joins - where you are repeatedly scanning/inspecting/manipulating entire columns of data - very common to the type of thing you would see in summary reports, Data Science, and ML. Typically these queries take minutes if not hours.  Photon can really help here.
# MAGIC 
# MAGIC - **Where Photon won't help much** - workloads where most time is spent outside of actual execution  - e.g. mostly spent on file I/O, just reading, writing, and filtering.  There's no math here, no aggregations nor joins, and no need for the massive SIMD parallelism - there's not much for Photon to add here.

# COMMAND ----------

# MAGIC %md
# MAGIC ##Enabling Photon
# MAGIC 
# MAGIC It's your choice whether to utilize the benefits of Photon - you get to choose when you startup the clusters for your notebooks, or your SQL endpoints.  Photon was designed to help specific Big Data calculation intensive workloads. We recommend you do your own benchmarking to decide whether or not to use it.  

# COMMAND ----------

# MAGIC %md
# MAGIC # Configure the environment
# MAGIC The following cell will create a database and source table that we'll use in this lesson, alongside some variables we'll use to control file locations.

# COMMAND ----------

# MAGIC %run ./Includes/setup $mode="reset"

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC DATABASE EXTENDED PhotonPerformance_mojgan_mazouchi_databricks_com_db

# COMMAND ----------

# MAGIC %sql
# MAGIC USE PhotonPerformance_mojgan_mazouchi_databricks_com_db

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/mojgan.mazouchi@databricks.com/PhotonPerformance

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get Raw data for Lending Club
# MAGIC It's included in every Databricks workspace. The data used is public data from Lending Club. It includes all funded loans from 2012 to 2017. Each loan includes applicant information provided by the applicant as well as the current loan status (Current, Late, Fully Paid, etc.) and latest payment information. For a full view of the data please view the data dictionary available [here](https://resources.lendingclub.com/LCDataDictionary.xlsx).
# MAGIC 
# MAGIC 
# MAGIC ![Loan_Data](https://preview.ibb.co/d3tQ4R/Screen_Shot_2018_02_02_at_11_21_51_PM.png)
# MAGIC 
# MAGIC https://www.kaggle.com/wendykan/lending-club-loan-data 

# COMMAND ----------

# DBTITLE 1,Locate raw data in DBFS
# MAGIC %fs ls "dbfs:/databricks-datasets/samples/lending_club/parquet"

# COMMAND ----------

# MAGIC %md
# MAGIC # Schemas
# MAGIC 
# MAGIC <img src='https://d9-wret.s3.us-west-2.amazonaws.com/assets/palladium/production/s3fs-public/styles/info_block/public/thumbnails/image/dm-file-formats.jpg?itok=2PE7A_QR' width="400">
# MAGIC 
# MAGIC 
# MAGIC The full schema of the origination and monthly performance data files are available on <a href="https://www.freddiemac.com/fmac-resources/research/pdf/user_guide.pdf">the user guide</a>. In this notebook, we can see some of the most relevant fields for mortgage loan performance monitoring.

# COMMAND ----------

# DBTITLE 1,Define the schema and ingestion dataframes
##
#  Setup a data set to create gzipped json files
##

import time
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timezone
import uuid

# get the schema from the parquet files

file_schema = (spark
               .read
               .format("parquet")
               .option("inferSchema", True)
               .load("dbfs:/databricks-datasets/samples/lending_club/parquet/*.parquet")
               .limit(10)
               .schema)

# COMMAND ----------

dfLendingClub = spark.read.format("parquet") \
  .schema(file_schema) \
  .load("dbfs:/databricks-datasets/samples/lending_club/parquet/*.parquet")

# COMMAND ----------

display(dfLendingClub)

# COMMAND ----------

dfLendingClub.write.mode("overwrite").saveAsTable("LendingClub")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL LendingClub

# COMMAND ----------

# DBTITLE 1,Make sure to avoid Side affects (No Cheating Demo Zone :-) ! ) 
# MAGIC %scala
# MAGIC 
# MAGIC //sc.setJobDescription("Step A-0: Basic initialization") for scala and pyspark
# MAGIC  
# MAGIC //Disabled to avoid side effects (reduce side affects)
# MAGIC spark.conf.set("spark.databricks.io.cache.enabled", "false")  
# MAGIC // spark.conf.set("spark.sql.adaptive.enabled", "false")
# MAGIC // spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")
# MAGIC // spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "false")

# COMMAND ----------

# DBTITLE 1,Find the distinct int_rate, will use this later to create a lookup dimension, so we have something to join to
dfLendingClub.select('int_rate').distinct().show()

# COMMAND ----------

# DBTITLE 1,Find the distinct payment_types, will use this later to create a lookup dimension, so we have something to join to
dfLendingClub.select('pymnt_plan').distinct().show()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) from LendingClub

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Silver Tables

# COMMAND ----------

# DBTITLE 1,Create IntRate dimension
# MAGIC %sql
# MAGIC USE PhotonPerformance_mojgan_mazouchi_databricks_com_db;
# MAGIC DROP TABLE IF EXISTS LendingClub_IntRate;
# MAGIC CREATE TABLE LendingClub_IntRate
# MAGIC USING DELTA 
# MAGIC AS
# MAGIC SELECT DISTINCT * 
# MAGIC FROM(
# MAGIC   SELECT int_rate, CASE WHEN int_rate IS NULL THEN "InvalidRate" 
# MAGIC   WHEN SUBSTRING(int_rate, 0, CHARINDEX('.', int_rate)-1) BETWEEN 0 AND 5 THEN "lowRate" 
# MAGIC   WHEN SUBSTRING(int_rate, 0, CHARINDEX('.', int_rate)-1) BETWEEN 5 AND 10 THEN "StandardRate" 
# MAGIC   WHEN SUBSTRING(int_rate, 0, CHARINDEX('.', int_rate)-1) BETWEEN 10 AND 15 THEN "MediumRate" 
# MAGIC   WHEN SUBSTRING(int_rate, 0, CHARINDEX('.', int_rate)-1) BETWEEN 15 AND 20 THEN "HighRate" 
# MAGIC   ELSE "ExtremelyHighRate" END as IntRate
# MAGIC   FROM LendingClub
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from LendingClub_IntRate 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM LendingClub_IntRate 

# COMMAND ----------

# DBTITLE 1,Create EmpLength dimension
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS LendingClub_EmpLength;
# MAGIC CREATE TABLE LendingClub_EmpLength 
# MAGIC AS
# MAGIC SELECT DISTINCT * 
# MAGIC FROM(
# MAGIC   SELECT emp_length, CASE WHEN SUBSTRING(emp_length, 0, CHARINDEX('+', emp_length)-1)==10 THEN "OverADecade" 
# MAGIC   WHEN SUBSTRING(emp_length, 0, CHARINDEX('years', emp_length)-1) BETWEEN 5 AND 10 THEN "5-9Years" 
# MAGIC   WHEN SUBSTRING(emp_length, 0, CHARINDEX('years', emp_length)-1) BETWEEN 3 AND 5 THEN "3-5Years" 
# MAGIC   WHEN SUBSTRING(emp_length, 0, CHARINDEX('years', emp_length)-1) BETWEEN 2 AND 3 THEN "2-3Years" 
# MAGIC   WHEN SUBSTRING(emp_length, 0, CHARINDEX('year', emp_length)-1) BETWEEN 1 AND 2 THEN "1year"
# MAGIC   WHEN SUBSTRING(emp_length, 3, CHARINDEX('<', emp_length))==1 THEN "Under1year" 
# MAGIC   ELSE "Unknown" END as EmpLength
# MAGIC   FROM LendingClub
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * from LendingClub_EmpLength 

# COMMAND ----------

# DBTITLE 1,Create curated Loan data, reduced for known Interest rates and employment length
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS LendingClub_silver;
# MAGIC CREATE TABLE LendingClub_silver 
# MAGIC AS
# MAGIC SELECT *
# MAGIC FROM LendingClub
# MAGIC WHERE
# MAGIC   (emp_length != 'n/a' AND emp_length IS NOT NULL)
# MAGIC   AND SUBSTRING(int_rate, 0, CHARINDEX('.', int_rate)-1) BETWEEN 0 AND 30

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from LendingClub_silver

# COMMAND ----------

dbutils.data.summarize(spark.table('LendingClub_silver'))

# COMMAND ----------

# MAGIC %md
# MAGIC #Start Testing (Join, Spill, Skew,...)

# COMMAND ----------

# MAGIC %sql
# MAGIC SET use_cached_result = false;
# MAGIC SELECT
# MAGIC   T_len.EmpLength,
# MAGIC   T_rate.IntRate,
# MAGIC   count(DISTINCT addr_state) loan_by_state,
# MAGIC   avg(loan_amnt) avg_loan_by_state,
# MAGIC   sum(total_pymnt) totalPayment_by_state
# MAGIC FROM
# MAGIC   LendingClub_silver T
# MAGIC   LEFT JOIN LendingClub_EmpLength T_len on T_len.emp_length = T.emp_length
# MAGIC   LEFT JOIN LendingClub_IntRate T_rate on T_rate.int_rate = T.int_rate
# MAGIC WHERE
# MAGIC   (annual_inc> 16000 and annual_inc< 100000) AND loan_status == 'Current'
# MAGIC GROUP BY
# MAGIC   1,
# MAGIC   2
# MAGIC HAVING EmpLength IN ('3-5Years', '1year', 'Under1year')
# MAGIC ORDER BY
# MAGIC   1, 
# MAGIC   2

# COMMAND ----------



