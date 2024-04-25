# Databricks notebook source
df  = spark.read.json("/Volumes/prod/bronze/file_storage/jokesapi")
df.createOrReplaceTempView("base")

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH data AS (
# MAGIC   SELECT DISTINCT
# MAGIC     category,
# MAGIC     delivery,
# MAGIC     flags.explicit,
# MAGIC     flags.nsfw,
# MAGIC     flags.political,
# MAGIC     flags.racist,
# MAGIC     flags.religious,
# MAGIC     flags.sexist,
# MAGIC     id,
# MAGIC     joke,
# MAGIC     lang,
# MAGIC     safe,
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
