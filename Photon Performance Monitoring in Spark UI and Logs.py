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
# MAGIC <img src='https://github.com/brickmeister/workshop_production_delta/blob/main/img/ganglia%20ui.png?raw=true'>

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


