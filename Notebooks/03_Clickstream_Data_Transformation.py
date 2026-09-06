# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

bronze_users = spark.table("workspace.ott_project.bronze_users")

bronze_movies = spark.table("workspace.ott_project.bronze_movies")

bronze_watch = spark.table("workspace.ott_project.bronze_watch_history")

bronze_search = spark.table("workspace.ott_project.bronze_search_logs")

bronze_recommend = spark.table("workspace.ott_project.bronze_recommendation_logs")

bronze_reviews = spark.table("workspace.ott_project.bronze_reviews")

# COMMAND ----------

display(bronze_watch)

# COMMAND ----------

silver_users = bronze_users.dropDuplicates()

# COMMAND ----------

silver_users.toPandas().head(10)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Calculate Median for age

# COMMAND ----------

median_age = silver_users.select(
    expr("percentile_approx(age, 0.5)")
).first()[0]

# COMMAND ----------

# MAGIC %md
# MAGIC #### Calculate Mean for monthly_spend

# COMMAND ----------

mean_monthly_spend = silver_users.select(
    mean("monthly_spend")
).first()[0]

# COMMAND ----------

silver_users = silver_users.withColumn(
    "monthly_spend",
    round(col("monthly_spend"), 2)
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Calculate Median for household_size

# COMMAND ----------

median_household = silver_users.select(
    expr("percentile_approx(household_size, 0.5)")
).first()[0]

# COMMAND ----------

# MAGIC %md
# MAGIC #### For missing Gender, gender is replaced with unknown

# COMMAND ----------

silver_users = silver_users.fillna({
    "gender": "Unknown"
})

# COMMAND ----------

silver_users = silver_users.fillna({
    "country": "Unknown",
    "gender": "Unknown",
    "subscription_plan": "Basic",
    "primary_device": "Unknown",
    "is_active": False,
    "age": median_age,
    "monthly_spend": mean_monthly_spend,
    "household_size": median_household
})

# COMMAND ----------

silver_users = silver_users.withColumn(
    "household_size",
    col("household_size").cast("int")
)

# COMMAND ----------

silver_users.toPandas().head(20)

# COMMAND ----------

silver_users = silver_users.filter(col("user_id").isNotNull())

# COMMAND ----------

silver_users = silver_users.withColumn(
    "country",
    upper(col("country"))
)

# COMMAND ----------

silver_users = silver_users.withColumn(
    "country",
    trim(col("country"))
)

# COMMAND ----------

silver_users.printSchema()

# COMMAND ----------

silver_users = silver_users.withColumn(
    "age",
    col("age").cast("int")
)

# COMMAND ----------

silver_users.write \
.mode("overwrite") \
.option("overwriteSchema", "true") \
.saveAsTable("workspace.ott_project.silver_users")

# COMMAND ----------

spark.table("workspace.ott_project.silver_users").toPandas()

# COMMAND ----------

silver_movies = bronze_movies.dropDuplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Filling genre_secondary missing values with "Unknown" 

# COMMAND ----------

silver_movies = silver_movies.fillna({
    "genre_secondary": "Unknown"
})

# COMMAND ----------

# MAGIC %md
# MAGIC #### imdb_rating missing values are filled with Mean

# COMMAND ----------

import builtins
from pyspark.sql.functions import mean

mean_rating = silver_movies.select(
    mean("imdb_rating")
).first()[0]

mean_rating = builtins.round(mean_rating, 1)

silver_movies = silver_movies.fillna({
    "imdb_rating": mean_rating
})

# COMMAND ----------

# MAGIC %md
# MAGIC #### Missing number_of_seasons are made 0

# COMMAND ----------

from pyspark.sql.functions import when, col

silver_movies = silver_movies.withColumn(
    "number_of_seasons",
    when(
        col("content_type") == "Movie",
        0
    ).otherwise(col("number_of_seasons"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Missing no of episodes are made 0

# COMMAND ----------

silver_movies = silver_movies.withColumn(
    "number_of_episodes",
    when(
        col("content_type") == "Movie",
        0
    ).otherwise(col("number_of_episodes"))
)

# COMMAND ----------

silver_movies = silver_movies.fillna({
    "genre_primary":"Unknown",
    "language":"Unknown"
})

# COMMAND ----------

silver_movies = silver_movies.withColumn(
    "genre_primary",
    upper(trim(col("genre_primary")))
)

# COMMAND ----------

silver_movies = silver_movies.withColumn(
    "duration_minutes",
    col("duration_minutes").cast("double")
)

# COMMAND ----------

silver_movies.write \
.mode("overwrite") \
.option("overwriteSchema","true") \
.saveAsTable("workspace.ott_project.silver_movies")

# COMMAND ----------

silver_movies.toPandas().head()

# COMMAND ----------

silver_watch_history = bronze_watch.dropDuplicates()

# COMMAND ----------

import builtins
from pyspark.sql.functions import mean

mean_watch_duration = silver_watch_history.select(
    mean("watch_duration_minutes")
).first()[0]

mean_watch_duration = builtins.round(mean_watch_duration, 2)

silver_watch_history = silver_watch_history.fillna({
    "watch_duration_minutes": mean_watch_duration
})

# COMMAND ----------

mean_progress = silver_watch_history.select(
    mean("progress_percentage")
).first()[0]

mean_progress = builtins.round(mean_progress, 2)

silver_watch_history = silver_watch_history.fillna({
    "progress_percentage": mean_progress
})

# COMMAND ----------

silver_watch_history = silver_watch_history.filter(
    col("session_id").isNotNull()
).filter(
    col("user_id").isNotNull()
).filter(
    col("movie_id").isNotNull()
)

# COMMAND ----------

from pyspark.sql.functions import upper, trim

silver_watch_history = silver_watch_history.withColumn(
    "device_type",
    upper(trim(col("device_type")))
)

silver_watch_history = silver_watch_history.withColumn(
    "quality",
    upper(trim(col("quality")))
)

silver_watch_history = silver_watch_history.withColumn(
    "location_country",
    upper(trim(col("location_country")))
)

silver_watch_history = silver_watch_history.withColumn(
    "action",
    upper(trim(col("action")))
)

# COMMAND ----------

silver_watch_history.write \
.mode("overwrite") \
.option("overwriteSchema","true") \
.saveAsTable("workspace.ott_project.silver_watch_history")

# COMMAND ----------

silver_watch_history.toPandas().head()

# COMMAND ----------

silver_search_logs = bronze_search.dropDuplicates()

# COMMAND ----------

from pyspark.sql.functions import col

silver_search_logs = silver_search_logs.filter(
    col("search_id").isNotNull()
).filter(
    col("user_id").isNotNull()
)

# COMMAND ----------

from pyspark.sql.functions import mean

mean_duration = silver_search_logs.select(
    mean("search_duration_seconds")
).first()[0]

silver_search_logs = silver_search_logs.fillna({
    "search_duration_seconds": mean_duration
})
silver_search_logs = silver_search_logs.withColumn(
    "search_duration_seconds",
    round(col("search_duration_seconds"), 2)
)

# COMMAND ----------

from pyspark.sql.functions import trim, upper

silver_search_logs = silver_search_logs.withColumn(
    "device_type",
    upper(trim(col("device_type")))
)

silver_search_logs = silver_search_logs.withColumn(
    "location_country",
    upper(trim(col("location_country")))
)

silver_search_logs = silver_search_logs.withColumn(
    "search_query",
    trim(col("search_query"))
)

# COMMAND ----------

from pyspark.sql.functions import col

silver_search_logs = silver_search_logs.withColumn(
    "clicked_result_position",
    col("clicked_result_position").cast("int")
)

# COMMAND ----------

silver_search_logs.printSchema()

# COMMAND ----------

silver_search_logs.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.ott_project.silver_search_logs")

# COMMAND ----------

silver_search_logs.toPandas().head()

# COMMAND ----------

silver_search_logs.select(
    "clicked_result_position"
).show()

# COMMAND ----------

spark.table("workspace.ott_project.silver_search_logs").show(5)

# COMMAND ----------

silver_recommendation_logs = bronze_recommend.dropDuplicates()

# COMMAND ----------

from pyspark.sql.functions import to_date, col

silver_recommendation_logs = silver_recommendation_logs.withColumn(
    "recommendation_date",
    to_date(col("recommendation_date"))
)

# COMMAND ----------

mean_score = silver_recommendation_logs.select(
    mean("recommendation_score")
).first()[0]

# COMMAND ----------

mean_score = builtins.round(mean_score, 3)

# COMMAND ----------

silver_recommendation_logs = silver_recommendation_logs.fillna({
    "recommendation_score": mean_score
})

# COMMAND ----------

silver_recommendation_logs = silver_recommendation_logs.fillna({
    "algorithm_version": "Unknown"
})

# COMMAND ----------

from pyspark.sql.functions import upper, trim

silver_recommendation_logs = silver_recommendation_logs.withColumn(
    "device_type",
    upper(trim(col("device_type")))
)

# COMMAND ----------

from pyspark.sql.functions import lower

silver_recommendation_logs = silver_recommendation_logs.withColumn(
    "recommendation_type",
    lower(trim(col("recommendation_type")))
)

# COMMAND ----------

silver_recommendation_logs = silver_recommendation_logs.withColumn(
    "time_of_day",
    lower(trim(col("time_of_day")))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validate Foreign keys

# COMMAND ----------

silver_recommendation_logs = silver_recommendation_logs.join(
    silver_users.select("user_id"),
    "user_id",
    "inner"
)

silver_recommendation_logs = silver_recommendation_logs.join(
    silver_movies.select("movie_id"),
    "movie_id",
    "inner"
)

# COMMAND ----------

silver_recommendation_logs.write \
.mode("overwrite") \
.option("overwriteSchema","true") \
.saveAsTable("workspace.ott_project.silver_recommendation_logs")

# COMMAND ----------

spark.table("workspace.ott_project.silver_recommendation_logs").show(5)

# COMMAND ----------

silver_recommendation_logs.toPandas().head()

# COMMAND ----------

silver_reviews = bronze_reviews.dropDuplicates()

# COMMAND ----------

from pyspark.sql.functions import col

silver_reviews = silver_reviews.filter(
    col("review_id").isNotNull()
).filter(
    col("user_id").isNotNull()
).filter(
    col("movie_id").isNotNull()
)

# COMMAND ----------

from pyspark.sql.functions import to_date

silver_reviews = silver_reviews.withColumn(
    "review_date",
    to_date(col("review_date"))
)

# COMMAND ----------

silver_reviews = silver_reviews.fillna({
    "helpful_votes": 0,
    "total_votes": 0
})

# COMMAND ----------

silver_reviews = silver_reviews.fillna({
    "review_text": "No Review"
})

# COMMAND ----------

from pyspark.sql.functions import mean

mean_score = silver_reviews.select(
    mean("sentiment_score")
).first()[0]

silver_reviews = silver_reviews.fillna({
    "sentiment_score": mean_score
})

# COMMAND ----------

from pyspark.sql.functions import round

silver_reviews = silver_reviews.withColumn(
    "sentiment_score",
    round(col("sentiment_score"), 3)
)

# COMMAND ----------

from pyspark.sql.functions import upper, trim

silver_reviews = silver_reviews.withColumn(
    "device_type",
    upper(trim(col("device_type")))
)

silver_reviews = silver_reviews.withColumn(
    "sentiment",
    upper(trim(col("sentiment")))
)

# COMMAND ----------

silver_reviews = silver_reviews.withColumn(
    "helpful_votes",
    col("helpful_votes").cast("int")
)

silver_reviews = silver_reviews.withColumn(
    "total_votes",
    col("total_votes").cast("int")
)

# COMMAND ----------

silver_reviews = silver_reviews.filter(
    (col("rating") >= 1) &
    (col("rating") <= 5)
)

# COMMAND ----------

silver_reviews = silver_reviews.withColumn(
    "review_text",
    trim(col("review_text"))
)

# COMMAND ----------

silver_reviews = silver_reviews.withColumn(
    "rating",
    col("rating").cast("double")
)

# COMMAND ----------

silver_reviews.write \
.mode("overwrite") \
.option("overwriteSchema","true") \
.saveAsTable("workspace.ott_project.silver_reviews")

# COMMAND ----------

spark.table("workspace.ott_project.silver_reviews").show(5)

# COMMAND ----------

silver_reviews.toPandas().head()