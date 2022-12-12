# Databricks notebook source
import pyspark.sql.functions as F
import re

demo = "PhotonPerformance"

username = spark.sql("SELECT current_user()").collect()[0][0]
userhome = f"dbfs:/user/{username}/{demo}"
database = f"""{demo}_{re.sub("[^a-zA-Z0-9]", "_", username)}_db"""

print(f"""
username: {username}
userhome: {userhome}
database: {database}""")

spark.sql(f"SET c.userhome = {userhome}")

dbutils.widgets.text("mode", "cleanup")
mode = dbutils.widgets.get("mode")


if mode == "reset":
    spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")
    dbutils.fs.rm(userhome, True)
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database} LOCATION '{userhome}'")
    spark.sql(f"USE {database}");

# COMMAND ----------

def check_files(table_name):
    filepath = spark.sql(f"DESCRIBE EXTENDED {table_name}").filter("col_name == 'Location'").select("data_type").collect()[0][0]
    filelist = dbutils.fs.ls(filepath)
    filecount = len([file for file in filelist if file.name != "_delta_log/" ])
    print(f"Count of all data files in {table_name}: {filecount}\n")
    return filelist

# COMMAND ----------

if mode == "cleanup":
    spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")
    dbutils.fs.rm(userhome, True)
