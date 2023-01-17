# Performance-with-Photon


Welcome to the repository for the Radical speed on the Lakehouse - Photon Workshop!
This repository contains the notebooks that are used in the workshop to demonstrate how and when to enable photon for additional performance gain which leads to the VM cost reduction, and finally how to monitor the performance of your jobs in the Spark UI when photon is enabled. 


# Reading Resources

* [Photon, The Next Generation Query Engine on the Databricks Lakehouse Platform](https://www.databricks.com/blog/2021/06/17/announcing-photon-public-preview-the-next-generation-query-engine-on-the-databricks-lakehouse-platform.html)
* [Faster MERGE Performance With Low-Shuffle MERGE and Photon](https://www.databricks.com/blog/2022/10/17/faster-merge-performance-low-shuffle-merge-and-photon.html)
* [Understanding your Apache Spark Application Through Visualization](https://www.databricks.com/blog/2015/06/22/understanding-your-spark-application-through-visualization.html)

# Workshop Flow

The workshop consists of 3 interactive sections that are separated by 3 notebooks located in the notebooks folder in this repository. Each is run sequentially as we explore how and when to use photon and how to monitor the performance gain achived with photon in spark UI.
|Notebook|Summary|
|--------|-------|
|`01-Lending Club Data Analysis With & Without Photon|Processing lending club dataset and running aggregations and Join queries with and without Photon to compare the processing time
|`02-Photon Performance Monitoring in Spark UI and Logs|Monitor and analyze Photon action in the Spark UI|
|`03-When-Photon-Doesnt-Help|showcase a workload that does not need to run on photon enabled cluster|



# Setup / Requirements

This workshop requires a running Databricks workspace. If you are an existing Databricks customer, you can use your existing Databricks workspace. 

## DBR Version

The notebooks used in this workshop require `DBR 11.3 LTS`.

## Repos

If you have repos enabled on your Databricks workspace. You can directly import this repo and run the notebooks as is.
