-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.fs.mkdirs("dbfs:/public/retail_db_bronze")
-- MAGIC dbutils.fs.mkdirs("dbfs:/public/retail_db_gold")

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/public

-- COMMAND ----------

-- MAGIC %run "./01 Cleanup Database and Datasets" $bronze_base_dir=/public/retail_db_bronze_v2 $gold_base_dir=/public/retail_db_gold/

-- COMMAND ----------

-- MAGIC %python
-- MAGIC bronze_base_dir

-- COMMAND ----------

-- MAGIC %python
-- MAGIC gold_base_dir

-- COMMAND ----------

-- MAGIC %run "./02 File Format Converter" $ds=orders $src_base_dir=/public/retail_db $bronze_base_dir=/public/retail_db_bronze_v2

-- COMMAND ----------

-- MAGIC %run "./02 File Format Converter" $ds=order_items $src_base_dir=/public/retail_db $bronze_base_dir=/public/retail_db_bronze_v2

-- COMMAND ----------

select * from PARQUET.`dbfs:/public/retail_db_bronze_v2/orders`

-- COMMAND ----------

-- MAGIC %run "./03 Create Spark SQL Tables" $table_name=orders $bronze_base_dir=/public/retail_db_bronze_v2

-- COMMAND ----------

-- MAGIC %run "./03 Create Spark SQL Tables" $table_name=order_items $bronze_base_dir=/public/retail_db_bronze_v2

-- COMMAND ----------

use itversity_retail_bronze

-- COMMAND ----------

SHOW tables

-- COMMAND ----------

SELECT count(*) FROM orders

-- COMMAND ----------

SELECT count(*) FROM order_items

-- COMMAND ----------

-- MAGIC %run "./04 Daily Product Revenue" $gold_base_dir=/public/retail_db_gold