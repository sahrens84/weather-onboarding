# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE prod.gold.jokes AS
# MAGIC   WITH base AS(
# MAGIC     SELECT *,
# MAGIC     ROW_NUMBER() OVER(PARTITION BY id ORDER BY valid_at DESC) AS row_number
# MAGIC     FROM prod.silver.jokes
# MAGIC   )
# MAGIC
# MAGIC   SELECT 
# MAGIC     * EXCEPT(row_number, valid_at), 
# MAGIC     DATE(valid_at) AS created_at 
# MAGIC   FROM base WHERE row_number = 1
