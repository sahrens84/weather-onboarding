# Databricks notebook source
df  = spark.read.json("/Volumes/prod/bronze/file_storage/jokesapi")
df.createOrReplaceTempView("base")

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH data AS (
# MAGIC   SELECT DISTINCT
# MAGIC     id,
# MAGIC     category,
# MAGIC     delivery,
# MAGIC     flags.explicit,
# MAGIC     flags.nsfw,
# MAGIC     flags.political,
# MAGIC     flags.racist,
# MAGIC     flags.religious,
# MAGIC     flags.sexist,
# MAGIC     joke,
# MAGIC     lang,
# MAGIC     safe AS is_safe,
# MAGIC     setup,
# MAGIC     type
# MAGIC   FROM base
# MAGIC )
# MAGIC
# MAGIC SELECT 
# MAGIC   data.*, 
# MAGIC   CURRENT_TIMESTAMP() AS valid_at 
# MAGIC FROM data

# COMMAND ----------

_sqldf.write.mode('append').saveAsTable("prod.silver.jokes") 
