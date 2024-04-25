# Databricks notebook source
from datetime import date
current_date = date.today().strftime("%d%m%Y")

# COMMAND ----------

df  = spark.read.json(f"/Volumes/prod/bronze/file_storage/weatherdotcom/{current_date}/")
df.createOrReplaceTempView("base")

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH data AS (
# MAGIC   SELECT DISTINCT
# MAGIC     current.* EXCEPT(condition),
# MAGIC     current.condition.*,
# MAGIC     location.* 
# MAGIC   FROM base
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   name AS city_name,
# MAGIC   region,
# MAGIC   country,
# MAGIC   tz_id,
# MAGIC   lat,
# MAGIC   lon,
# MAGIC   cloud,
# MAGIC   feelslike_c,
# MAGIC   feelslike_f,
# MAGIC   gust_kph,
# MAGIC   gust_mph,
# MAGIC   humidity,
# MAGIC   is_day::BOOLEAN,
# MAGIC   last_updated::TIMESTAMP,
# MAGIC   last_updated_epoch,
# MAGIC   precip_in,
# MAGIC   precip_mm,
# MAGIC   pressure_in,
# MAGIC   pressure_mb,
# MAGIC   temp_c,
# MAGIC   temp_f,
# MAGIC   uv,
# MAGIC   vis_km,
# MAGIC   vis_miles,
# MAGIC   wind_degree,
# MAGIC   wind_dir,
# MAGIC   wind_kph,
# MAGIC   wind_mph,
# MAGIC   code,
# MAGIC   icon,
# MAGIC   text,
# MAGIC   localtime::TIMESTAMP,
# MAGIC   localtime_epoch,
# MAGIC   CURRENT_TIMESTAMP() AS valid_at
# MAGIC FROM data

# COMMAND ----------

_sqldf.write.mode('append').saveAsTable("prod.silver.city_weather")
