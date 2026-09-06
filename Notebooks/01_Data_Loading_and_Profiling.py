# Databricks notebook source
spark.sql("SHOW TABLES IN workspace.ott_project").show(truncate=False)

# COMMAND ----------

users_df = spark.table("workspace.ott_project.users")

movies_df = spark.table("workspace.ott_project.movies")

watch_df = spark.table("workspace.ott_project.watch_history")

search_df = spark.table("workspace.ott_project.search_logs")

recommend_df = spark.table("workspace.ott_project.recommendation_logs")

reviews_df = spark.table("workspace.ott_project.reviews")

# COMMAND ----------

users_df.show(5)

# COMMAND ----------

movies_df.show(5)

# COMMAND ----------

watch_df.show(5)

# COMMAND ----------

print("Users:", users_df.count())

print("Movies:", movies_df.count())

print("Watch History:", watch_df.count())

print("Search Logs:", search_df.count())

print("Recommendations:", recommend_df.count())

print("Reviews:", reviews_df.count())

# COMMAND ----------

users_df.printSchema()

# COMMAND ----------

movies_df.printSchema()

# COMMAND ----------

watch_df.printSchema()

# COMMAND ----------

search_df.printSchema()

# COMMAND ----------

recommend_df.printSchema()

# COMMAND ----------

reviews_df.printSchema()

# COMMAND ----------

users_df.describe().show()

# COMMAND ----------

movies_df.describe().show()

# COMMAND ----------

watch_df.describe().show()

# COMMAND ----------

from pyspark.sql.functions import col, count, when

users_df.select([
    count(
        when(col(c).isNull(), c)
    ).alias(c)
    for c in users_df.columns
]).show()

# COMMAND ----------

spark.sql("SHOW TABLES IN workspace.ott_project").show(truncate=False)