# Databricks notebook source
import pyspark.sql.functions as F
import matplotlib.pyplot as plt

# Load jokes data from the gold layer (assuming it's stored in a table named 'gold_jokes')
jokes_df = spark.table("prod.gold.jokes")

# COMMAND ----------

# Average length of all jokes
average_joke_length = jokes_df.select(
    F.avg(
        F.when(jokes_df.type == "single", F.length("joke"))
         .when(jokes_df.type == "twopart", F.length(F.concat_ws(" ", "setup", "delivery")))
    ).alias("average_joke_length")
).first()[0]
dbutils.widgets.text("Average Length of All Jokes:", str(round(average_joke_length)))

# COMMAND ----------

# Distribution of joke categories for pie chart
categories_counts = jokes_df.groupBy("category").count()
categories_df = categories_counts.toPandas()
display(categories_df)

# COMMAND ----------

# Total amount of jokes
total_jokes_count = jokes_df.count()
dbutils.widgets.text("Total amount of jokes:", str(total_jokes_count))

# COMMAND ----------

# Maximum created_at date
max_created_at = jokes_df.agg(F.max("created_at")).first()[0]
dbutils.widgets.text("Maximum joke creation date:", str(max_created_at))
