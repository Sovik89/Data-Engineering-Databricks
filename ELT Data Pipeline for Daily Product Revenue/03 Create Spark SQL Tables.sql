-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS itversity_retail_bronze

-- COMMAND ----------

USE itversity_retail_bronze

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.removeAll()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.text("table_name", "", "Table Name")
-- MAGIC dbutils.widgets.text("bronze_base_dir", "", "Bronze Base Directory")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC bronze_base_dir=dbutils.widgets.get('bronze_base_dir')
-- MAGIC table_name=dbutils.widgets.get('table_name')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(f'dbfs:{bronze_base_dir}/{table_name}')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.parquet(f'dbfs:{bronze_base_dir}/{table_name}/')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.mode('overwrite').saveAsTable(f'{table_name}')

-- COMMAND ----------

show tables