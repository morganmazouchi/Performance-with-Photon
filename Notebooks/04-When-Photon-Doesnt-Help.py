# Databricks notebook source
# MAGIC %python
# MAGIC 
# MAGIC """
# MAGIC Setup up some objects
# MAGIC """
# MAGIC 
# MAGIC from pyspark.sql.functions import schema_of_json
# MAGIC 
# MAGIC _json = """{
# MAGIC    "store":{
# MAGIC       "fruit": [
# MAGIC         {"weight":8,"type":"apple"},
# MAGIC         {"weight":9,"type":"pear"}
# MAGIC       ],
# MAGIC       "basket":[
# MAGIC         [1,2,{"b":"y","a":"x"}],
# MAGIC         [3,4],
# MAGIC         [5,6]
# MAGIC       ],
# MAGIC       "book":[
# MAGIC         {
# MAGIC           "author":"Nigel Rees",
# MAGIC           "title":"Sayings of the Century",
# MAGIC           "category":"reference",
# MAGIC           "price":8.95
# MAGIC         },
# MAGIC         {
# MAGIC           "author":"Herman Melville",
# MAGIC           "title":"Moby Dick",
# MAGIC           "category":"fiction",
# MAGIC           "price":8.99,
# MAGIC           "isbn":"0-553-21311-3"
# MAGIC         },
# MAGIC         {
# MAGIC           "author":"J. R. R. Tolkien",
# MAGIC           "title":"The Lord of the Rings",
# MAGIC           "category":"fiction",
# MAGIC           "reader":[
# MAGIC             {"age":25,"name":"bob"},
# MAGIC             {"age":26,"name":"jack"}
# MAGIC           ],
# MAGIC           "price":22.99,
# MAGIC           "isbn":"0-395-19395-8"
# MAGIC         }
# MAGIC       ],
# MAGIC       "bicycle":{
# MAGIC         "price":19.95,
# MAGIC         "color":"red"
# MAGIC       }
# MAGIC     },
# MAGIC     "owner":"mark",
# MAGIC     "zip code":"94025"
# MAGIC  }"""
# MAGIC 
# MAGIC # get the schema of the json
# MAGIC _schema = spark.sql(f"select schema_of_json('{_json}') as schema").collect()[0][0]
# MAGIC print(_schema)

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC """
# MAGIC Create a nested json table
# MAGIC Code is taken from https://docs.databricks.com/optimizations/semi-structured.html#create-a-table-with-highly-nested-data
# MAGIC """
# MAGIC 
# MAGIC display(spark.sql(f"""CREATE OR REPLACE TABLE store_data
# MAGIC     USING delta
# MAGIC     TBLPROPERTIES('delta.columnMapping.mode' = 'name',
# MAGIC                   'delta.minReaderVersion' = '2',
# MAGIC                   'delta.minWriterVersion' = '5')
# MAGIC     AS 
# MAGIC       SELECT from_json('{_json}', '{_schema}') as raw
# MAGIC  """))

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC """
# MAGIC Multiply data
# MAGIC """
# MAGIC 
# MAGIC for i in range(10):
# MAGIC   spark.sql("INSERT INTO store_data SELECT * FROM store_data")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --
# MAGIC -- Visualize the nested json data
# MAGIC --
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM store_data;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --
# MAGIC -- Calculate the sum of the average age for all readers
# MAGIC --
# MAGIC 
# MAGIC WITH _DATA AS (
# MAGIC   SELECT explode(raw.store.book.reader.age[2]) as age,
# MAGIC          books.category as category
# MAGIC   FROM store_data
# MAGIC   LATERAL VIEW explode(raw.store.book) as books
# MAGIC   )
# MAGIC   SELECT category,
# MAGIC          avg(age) as average_age,
# MAGIC          stddev(age) as sd_age
# MAGIC   FROM _DATA
# MAGIC   where lower(category) in ('reference', 'fiction')
# MAGIC   group by category;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --
# MAGIC -- Calculate the sum of the average age for all readers (no photon)
# MAGIC --
# MAGIC 
# MAGIC WITH _DATA AS (
# MAGIC   SELECT explode(raw.store.book.reader.age[2]) as age,
# MAGIC          books.category as category
# MAGIC   FROM store_data
# MAGIC   LATERAL VIEW explode(raw.store.book) as books
# MAGIC   )
# MAGIC   SELECT category,
# MAGIC          avg(age) as average_age,
# MAGIC          stddev(age) as sd_age
# MAGIC   FROM _DATA
# MAGIC   where lower(category) in ('reference', 'fiction')
# MAGIC   group by category;
