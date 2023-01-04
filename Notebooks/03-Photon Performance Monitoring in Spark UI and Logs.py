# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Monitoring Performance of Jobs
# MAGIC 
# MAGIC Optimizing performance from a job and cluster perspective is a critical part of Production Delta Lake Performance. We often use a variety of methods listed in the table below to monitor jobs performance.
# MAGIC 
# MAGIC |Feature|Use|Link|
# MAGIC |-------|---|----|
# MAGIC |Ganglia Metrics: Cluster Tuning | Controlling the knobs associated with seeking out maximal performance|https://docs.databricks.com/clusters/configure.html|
# MAGIC |Spark UI: DAG Tuning | Controlling the stages associated with a Spark Job and a Query Execution Plan | https://databricks.com/session/understanding-query-plans-and-spark-uis|
# MAGIC |Logs | Spark Driver and Cluster Logs that provide execution and autoscaling details | https://docs.databricks.com/spark/latest/rdd-streaming/debugging-streaming-applications.html|
# MAGIC |EXPLAIN: Physical Plan | physical plan provides the fundamental information about the execution of the query |https://docs.databricks.com/sql/language-manual/sql-ref-syntax-qry-explain.html |
# MAGIC 
# MAGIC Useful blogs to understand how Delta Lake performance tuning works are listed below.
# MAGIC * [Photon, The Next Generation Query Engine on the Databricks Lakehouse Platform](https://www.databricks.com/blog/2021/06/17/announcing-photon-public-preview-the-next-generation-query-engine-on-the-databricks-lakehouse-platform.html)
# MAGIC * [Faster MERGE Performance With Low-Shuffle MERGE and Photon](https://www.databricks.com/blog/2022/10/17/faster-merge-performance-low-shuffle-merge-and-photon.html)
# MAGIC * [Understanding your Apache Spark Application Through Visualization](https://www.databricks.com/blog/2015/06/22/understanding-your-spark-application-through-visualization.html)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Cluster Performance
# MAGIC 
# MAGIC Cluster performance tuning is an important step when quantifying delta performance. The distributed nature of Delta and Spark allow great horizontal scaling by adding more nodes to meet performance SLAs. Generally speaking, leverage autoscaling on Spark clusters to reduce costs and tackle peak throughput workloads.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Live Metrics - Ganglia UI
# MAGIC 
# MAGIC Databricks cluster performance can be observed in the Ganglia UI which runs live on the cluster.
# MAGIC 
# MAGIC <img src='https://raw.githubusercontent.com/morganmazouchi/Performance-with-Photon/main/Images/Ganglia%20UI.png' width="1500">

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Spark UI
# MAGIC 
# MAGIC Databricks exposes the Spark UI which will provide a large amount of usable statistics to measure the performance of your jobs. Every job in Spark consists of a series of spark tasks (stages) which form a directed acyclic graph (DAG). Examining these DAGs can help identify bottleneck stages to determine where more performance can be extracted. 
# MAGIC 
# MAGIC <img src='https://raw.githubusercontent.com/morganmazouchi/Performance-with-Photon/main/Images/Spark%20UI-%20Job%20159.png' width="800">
# MAGIC <img src='https://raw.githubusercontent.com/morganmazouchi/Performance-with-Photon/main/Images/Spark%20UI-%20Job%20166%20-%20No%20Photon.png' width="1300">

# COMMAND ----------

# DBTITLE 0,Speed up queries by identifying execution bottlenecks in Query Plans
# MAGIC %md
# MAGIC 
# MAGIC ## Speed up queries by identifying execution bottlenecks in Query Plans
# MAGIC A common methodology for speeding up queries is to first identify the longest running query operators. We are more interested in total time spent on a task rather than the exact “wall clock time” of an operator as we’re dealing with a distributed system and operators can be executed in parallel. Each query operator comes with a slew of statistics. In the case of a scan operator, metrics include number of files or data read, time spent waiting for cloud storage or time spent reading files. As a result, it is easy to answer questions such as which table should be optimized or whether a join could be improved. All blue DAGs in the query plan confirms that photon was disabled when the query ran. 
# MAGIC 
# MAGIC <img src='https://raw.githubusercontent.com/morganmazouchi/Performance-with-Photon/main/Images/Databricks%20Shell%20-%20Details%20for%20Query%20299.png' width="1200">

# COMMAND ----------

spark.conf.set("spark.databricks.photon.enabled", "false")
spark.conf.set("spark.databricks.photon.parquetWriter.enabled", "false")
spark.conf.set("spark.databricks.photon.window.enabled", "false")
spark.conf.set("spark.databricks.photon.sort.enabled", "false")
spark.conf.set("spark.databricks.photon.window.experimental.features.enabled", "false")

# COMMAND ----------

# DBTITLE 1,Run Explain when Photon is Disabled
# MAGIC %scala
# MAGIC spark.sql("""EXPLAIN SELECT
# MAGIC   T_len.EmpLength,
# MAGIC   T_rate.IntRate,
# MAGIC   count(DISTINCT T.addr_state) cnt_loan_by_state,
# MAGIC   avg(loan_amnt) avg_loan_by_state,
# MAGIC   min(DISTINCT annual_inc) as min_annual_income,
# MAGIC   max(DISTINCT annual_inc) as max_annual_income,
# MAGIC   sum(total_pymnt) totalPayment_by_state
# MAGIC FROM
# MAGIC   PhotonPerformance_mojgan_mazouchi_databricks_com_db.LendingClub_silver T
# MAGIC   LEFT JOIN 
# MAGIC   (SELECT row_number() OVER(PARTITION BY addr_state ORDER BY avg_cur_bal DESC) as row_num_avgBal_state, *
# MAGIC   FROM PhotonPerformance_mojgan_mazouchi_databricks_com_db.LendingClub_EmpLength) T_len on T_len.emp_length = T.emp_length and T_len.avg_cur_bal BETWEEN 1 AND 2000
# MAGIC   LEFT JOIN PhotonPerformance_mojgan_mazouchi_databricks_com_db.LendingClub_IntRate T_rate on T_rate.int_rate = T.int_rate
# MAGIC WHERE
# MAGIC   (annual_inc> 16000) AND loan_status == "Current"
# MAGIC GROUP BY
# MAGIC   1,
# MAGIC   2
# MAGIC HAVING EmpLength IN ('3-5Years', '1year', 'Under1year')""").collect().foreach(println)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Photon execution analysis in Query Plan
# MAGIC If you are using Photon on Databricks clusters, you can view Photon action in the Spark UI. The following screenshot shows the query details DAG. There are two indications of Photon in the DAG. First, Photon operators start with Photon, such as PhotonGroupingAgg. Secondly, in the DAG Photon operators and stages are colored orange, whereas the non-Photon ones are blue.
# MAGIC 
# MAGIC <img src='https://raw.githubusercontent.com/morganmazouchi/Performance-with-Photon/main/Images/Databricks%20Shell%20-%20Details%20for%20Query%20272.png' width="1800">

# COMMAND ----------

#Enable photon and it's support for sort and window functions
spark.conf.set("spark.databricks.photon.enabled", "true")
spark.conf.set("spark.databricks.photon.parquetWriter.enabled", "true")
spark.conf.set("spark.databricks.photon.window.enabled", "true")
spark.conf.set("spark.databricks.photon.sort.enabled", "true")
spark.conf.set("spark.databricks.photon.window.experimental.features.enabled", "true")

# COMMAND ----------

# DBTITLE 1,Run Explain on Photon-Enabled Cluster
# MAGIC %scala
# MAGIC spark.sql("""EXPLAIN SELECT
# MAGIC   T_len.EmpLength,
# MAGIC   T_rate.IntRate,
# MAGIC   count(DISTINCT T.addr_state) cnt_loan_by_state,
# MAGIC   avg(loan_amnt) avg_loan_by_state,
# MAGIC   min(DISTINCT annual_inc) as min_annual_income,
# MAGIC   max(DISTINCT annual_inc) as max_annual_income,
# MAGIC   sum(total_pymnt) totalPayment_by_state
# MAGIC FROM
# MAGIC   PhotonPerformance_mojgan_mazouchi_databricks_com_db.LendingClub_silver T
# MAGIC   LEFT JOIN 
# MAGIC   (SELECT row_number() OVER(PARTITION BY addr_state ORDER BY avg_cur_bal DESC) as row_num_avgBal_state, *
# MAGIC   FROM PhotonPerformance_mojgan_mazouchi_databricks_com_db.LendingClub_EmpLength) T_len on T_len.emp_length = T.emp_length and T_len.avg_cur_bal BETWEEN 1 AND 2000
# MAGIC   LEFT JOIN PhotonPerformance_mojgan_mazouchi_databricks_com_db.LendingClub_IntRate T_rate on T_rate.int_rate = T.int_rate
# MAGIC WHERE
# MAGIC   (annual_inc> 16000) AND loan_status == "Current"
# MAGIC GROUP BY
# MAGIC   1,
# MAGIC   2
# MAGIC HAVING EmpLength IN ('3-5Years', '1year', 'Under1year')""").collect().foreach(println)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Photon-Enabled Clusters
# MAGIC 
# MAGIC By enabling the advice text (`set spark.databricks.adviceGenerator.acceleratedWithPhoton.enabled = true;`), you can trace photon-enabled clusters logs in the INFO section of Driver logs under Log4j output. Look specifically for "Accelerated with photon" in the logs to find out how much your queries and workloads accelerated by photon.
# MAGIC 
# MAGIC <img alt="Caution" title="Caution" style="vertical-align: text-bottom; position: relative; height:1.3em; top:0.0em" src="https://files.training.databricks.com/static/images/icon-warning.svg"/> 
# MAGIC ** Advice text is disabled by default**, and you have to enable it in advance, prior to running your queries.

# COMMAND ----------

# MAGIC %md
# MAGIC <img src='https://raw.githubusercontent.com/morganmazouchi/Performance-with-Photon/main/Images/log%204j%20output.png' width="2500">

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC set spark.databricks.adviceGenerator.acceleratedWithPhoton.enabled = true;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   T_len.EmpLength,
# MAGIC   T_rate.IntRate,
# MAGIC   count(DISTINCT T.addr_state) cnt_loan_by_state,
# MAGIC   avg(loan_amnt) avg_loan_by_state,
# MAGIC   min(DISTINCT annual_inc) as min_annual_income,
# MAGIC   max(DISTINCT annual_inc) as max_annual_income,
# MAGIC   sum(total_pymnt) totalPayment_by_state
# MAGIC FROM
# MAGIC   PhotonPerformance_mojgan_mazouchi_databricks_com_db.LendingClub_silver T
# MAGIC   LEFT JOIN 
# MAGIC   (SELECT row_number() OVER(PARTITION BY addr_state ORDER BY avg_cur_bal DESC) as row_num_avgBal_state, *
# MAGIC   FROM PhotonPerformance_mojgan_mazouchi_databricks_com_db.LendingClub_EmpLength) T_len on T_len.emp_length = T.emp_length and T_len.avg_cur_bal BETWEEN 1 AND 10
# MAGIC   LEFT JOIN PhotonPerformance_mojgan_mazouchi_databricks_com_db.LendingClub_IntRate T_rate on T_rate.int_rate = T.int_rate
# MAGIC WHERE
# MAGIC   (annual_inc> 16000) AND loan_status == "Current"
# MAGIC GROUP BY
# MAGIC   1,
# MAGIC   2
# MAGIC HAVING EmpLength IN ('3-5Years', '1year', 'Under1year')
