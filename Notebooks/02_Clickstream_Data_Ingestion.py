# Databricks notebook source
users_df = spark.table("workspace.ott_project.users")

movies_df = spark.table("workspace.ott_project.movies")

watch_df = spark.read.parquet("/Volumes/workspace/ott_project/silver_input")

search_df = spark.table("workspace.ott_project.search_logs")

recommend_df = spark.table("workspace.ott_project.recommendation_logs")

reviews_df = spark.table("workspace.ott_project.reviews")

# COMMAND ----------

from pyspark.sql.functions import col, to_date, to_timestamp

watch_df = (
    watch_df
    .withColumn("watch_date", to_date(col("watch_date"), "yyyy-MM-dd"))
    .withColumn("load_date", to_date(col("load_date"), "yyyy-MM-dd"))
    .withColumn("ingestion_timestamp", to_timestamp(col("ingestion_timestamp")))
)

# COMMAND ----------

users_df.write \
    .mode("overwrite") \
    .saveAsTable("workspace.ott_project.bronze_users")

# COMMAND ----------

movies_df.write \
    .mode("overwrite") \
    .saveAsTable("workspace.ott_project.bronze_movies")

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS workspace.ott_project.bronze_watch_history")

# COMMAND ----------

watch_df.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("workspace.ott_project.bronze_watch_history")

# COMMAND ----------

search_df.write \
    .mode("overwrite") \
    .saveAsTable("workspace.ott_project.bronze_search_logs")

# COMMAND ----------

recommend_df.write \
    .mode("overwrite") \
    .saveAsTable("workspace.ott_project.bronze_recommendation_logs")

# COMMAND ----------

reviews_df.write \
    .mode("overwrite") \
    .saveAsTable("workspace.ott_project.bronze_reviews")

# COMMAND ----------

spark.sql("SHOW TABLES IN workspace.ott_project").show(truncate=False)

# COMMAND ----------

bronze_users = spark.table("workspace.ott_project.bronze_users")

bronze_users.show(5)

# COMMAND ----------

users_df.show(5)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, current_date, lit

# COMMAND ----------

batch_id = "BATCH_001"

# COMMAND ----------

users_df.show(5)

# COMMAND ----------

bronze_users = (
    users_df
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_file", lit("users.csv"))
    .withColumn("load_date", current_date())
    .withColumn("batch_id", lit("BATCH_001"))
)

# COMMAND ----------

bronze_users.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.ott_project.bronze_users")

# COMMAND ----------

spark.table("workspace.ott_project.bronze_users").printSchema()

# COMMAND ----------

bronze_movies = (
    movies_df
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_file", lit("movies.csv"))
    .withColumn("load_date", current_date())
    .withColumn("batch_id", lit(batch_id))
)

bronze_movies.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.ott_project.bronze_movies")

# COMMAND ----------

bronze_watch_history = (
    watch_df
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_file", lit("watch_history.csv"))
    .withColumn("load_date", current_date())
    .withColumn("batch_id", lit(batch_id))
)

bronze_watch_history.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.ott_project.bronze_watch_history")

# COMMAND ----------

# Create a Volume inside your catalog/schema
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.ott_project.checkpoints")

# COMMAND ----------

bronze_search_logs = (
    search_df
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_file", lit("search_logs.csv"))
    .withColumn("load_date", current_date())
    .withColumn("batch_id", lit(batch_id))
)

bronze_search_logs.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.ott_project.bronze_search_logs")

# COMMAND ----------

bronze_recommendation_logs = (
    recommend_df
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_file", lit("recommendation_logs.csv"))
    .withColumn("load_date", current_date())
    .withColumn("batch_id", lit(batch_id))
)

bronze_recommendation_logs.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.ott_project.bronze_recommendation_logs")

# COMMAND ----------

bronze_reviews = (
    reviews_df
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_file", lit("reviews.csv"))
    .withColumn("load_date", current_date())
    .withColumn("batch_id", lit(batch_id))
)

bronze_reviews.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.ott_project.bronze_reviews")

# COMMAND ----------

spark.sql("SHOW TABLES IN workspace.ott_project").show(truncate=False)

# COMMAND ----------

spark.table("workspace.ott_project.bronze_movies").printSchema()

# COMMAND ----------

spark.sql("SHOW TABLES IN workspace.ott_project").show(truncate=False)

# COMMAND ----------

bronze_df = spark.table("workspace.ott_project.bronze_watch_history")

display(bronze_df)

# COMMAND ----------

spark.sql("SHOW VOLUMES IN workspace.ott_project").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP VOLUME workspace.ott_project.bronze_export;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME workspace.ott_project.bronze_export;

# COMMAND ----------

bronze_df = spark.table("workspace.ott_project.bronze_watch_history")

(bronze_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", "true")
    .csv("/Volumes/workspace/ott_project/bronze_export"))

# COMMAND ----------

display(dbutils.fs.ls("/Volumes/workspace/ott_project/bronze_export"))

# COMMAND ----------

watch_df.printSchema()

# COMMAND ----------

spark.table("workspace.ott_project.bronze_watch_history").printSchema()

# COMMAND ----------

