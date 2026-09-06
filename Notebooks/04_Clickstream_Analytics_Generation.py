# Databricks notebook source
silver_users = spark.table("workspace.ott_project.silver_users")

silver_movies = spark.table("workspace.ott_project.silver_movies")

silver_watch_history = spark.table("workspace.ott_project.silver_watch_history")

silver_search_logs = spark.table("workspace.ott_project.silver_search_logs")

silver_recommendation_logs = spark.table("workspace.ott_project.silver_recommendation_logs")

silver_reviews = spark.table("workspace.ott_project.silver_reviews")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Business Data
# MAGIC
# MAGIC Gold Table 1 - OTT Clickstream Analytics

# COMMAND ----------

# MAGIC %md
# MAGIC #### Q1) How much content is watched every day?

# COMMAND ----------

from pyspark.sql.functions import *

gold_daily_watch = silver_watch_history.groupBy("watch_date") \
.agg(
    count("session_id").alias("total_sessions"),
    countDistinct("user_id").alias("active_users"),
    round(sum("watch_duration_minutes"),2).alias("watch_minutes"),
    round(avg("watch_duration_minutes"),2).alias("avg_watch_duration"),
    round(avg("progress_percentage"),2).alias("avg_completion")
)

# COMMAND ----------

gold_daily_watch.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema","true") \
    .saveAsTable("workspace.ott_project.gold_daily_watch")

# COMMAND ----------

print(
    spark.table("workspace.ott_project.gold_daily_watch").count()
)

# COMMAND ----------

display(gold_daily_watch)

# COMMAND ----------

# MAGIC %md
# MAGIC Gold Table 2 - Genre Popularity

# COMMAND ----------

# MAGIC %md
# MAGIC #### Q2) Which genres are most watched?

# COMMAND ----------

gold_genre = silver_watch_history.join(
    silver_movies,
    "movie_id"
).groupBy(
    "genre_primary"
).agg(
    count("session_id").alias("views"),
    round(sum("watch_duration_minutes"),2).alias("watch_minutes"),
    round(avg("progress_percentage"),2).alias("completion_rate")
)

# COMMAND ----------

gold_genre.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema","true") \
    .saveAsTable("workspace.ott_project.gold_genre")

# COMMAND ----------

print(
    spark.table("workspace.ott_project.gold_genre").count()
)

# COMMAND ----------

display(gold_genre)

# COMMAND ----------

# MAGIC %md
# MAGIC Gold Table 3 - Device Behaviour Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC #### Q3) Which devices do users prefer?

# COMMAND ----------

gold_device = silver_watch_history.groupBy(
    "device_type"
).agg(
    count("session_id").alias("sessions"),
    round(sum("watch_duration_minutes"),2).alias("watch_time"),
    round(avg("progress_percentage"),2).alias("completion")
)

# COMMAND ----------

gold_device.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema","true") \
    .saveAsTable("workspace.ott_project.gold_device")

# COMMAND ----------

print(
    spark.table("workspace.ott_project.gold_device").count()
)

# COMMAND ----------

display(gold_device)

# COMMAND ----------

# MAGIC %md
# MAGIC Gold Table 4 - Search Behaviour Analytics (Clickstream)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Q4) What do users search for? How long do they search? Are searches successful?

# COMMAND ----------

gold_search = silver_search_logs.groupBy(
    "search_query"
).agg(
    count("*").alias("search_count"),
    round(avg("search_duration_seconds"),2).alias("avg_search_time"),
    sum("results_returned").alias("results_returned")
)

# COMMAND ----------

gold_search.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema","true") \
    .saveAsTable("workspace.ott_project.gold_search")

# COMMAND ----------

print(
    spark.table("workspace.ott_project.gold_search").count()
)

# COMMAND ----------

display(gold_search)

# COMMAND ----------

# MAGIC %md
# MAGIC Gold Table 5 - Recommendation Behaviour

# COMMAND ----------

# MAGIC %md
# MAGIC #### Q5) How effective is the recommendation engine?

# COMMAND ----------

gold_recommendation = silver_recommendation_logs.groupBy(
    "recommendation_type"
).agg(
    count("*").alias("recommendations"),
    round(avg("recommendation_score"),3).alias("avg_score"),
    sum(
        when(col("was_clicked")==True,1).otherwise(0)
    ).alias("clicked")
)

# COMMAND ----------

gold_recommendation.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema","true") \
    .saveAsTable("workspace.ott_project.gold_recommendation")

# COMMAND ----------

print(
    spark.table("workspace.ott_project.gold_recommendation").count()
)

# COMMAND ----------

display(gold_recommendation)

# COMMAND ----------

# MAGIC %md
# MAGIC Gold Table 6 - User Behaviour Analytics

# COMMAND ----------

# MAGIC %md
# MAGIC #### Q6) Which users are most engaged?

# COMMAND ----------

gold_user = silver_watch_history.join(
    silver_users,
    "user_id"
).groupBy(
    "subscription_plan",
    "country"
).agg(
    count("session_id").alias("sessions"),
    round(sum("watch_duration_minutes"),2).alias("watch_time"),
    countDistinct("user_id").alias("users")
)

# COMMAND ----------

gold_user.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema","true") \
    .saveAsTable("workspace.ott_project.gold_user")

# COMMAND ----------

print(
    spark.table("workspace.ott_project.gold_user").count()
)

# COMMAND ----------

display(gold_user)

# COMMAND ----------

# MAGIC %md
# MAGIC Gold Table 7 - Review Analytics

# COMMAND ----------

# MAGIC %md
# MAGIC #### Q7) How are users rating the platform?

# COMMAND ----------

from pyspark.sql.functions import avg, sum, round, col

gold_reviews = silver_reviews.alias("r") \
    .join(
        silver_movies.alias("m"),
        col("r.movie_id") == col("m.movie_id")
    ) \
    .groupBy("m.genre_primary") \
    .agg(
        round(avg(col("r.rating")), 2).alias("avg_rating"),
        round(avg(col("r.sentiment_score")), 3).alias("avg_sentiment"),
        sum(col("r.helpful_votes")).alias("helpful_votes")
    )

# COMMAND ----------

gold_reviews.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema","true") \
    .saveAsTable("workspace.ott_project.gold_reviews")

# COMMAND ----------

print(
    spark.table("workspace.ott_project.gold_reviews").count()
)

# COMMAND ----------

display(gold_reviews)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Q8) Which content performs the best on the OTT platform?

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Tables Used
# MAGIC silver_movies
# MAGIC
# MAGIC silver_watch_history
# MAGIC
# MAGIC silver_reviews
# MAGIC
# MAGIC #### Metrics used - 
# MAGIC Total Views
# MAGIC
# MAGIC Total Watch Time
# MAGIC
# MAGIC Average Completion %
# MAGIC
# MAGIC Average User Rating
# MAGIC
# MAGIC Average Sentiment Score

# COMMAND ----------

from pyspark.sql.functions import col, count, sum, avg, round

gold_content_performance = (
    silver_watch_history.alias("w")
    .join(
        silver_movies.alias("m"),
        col("w.movie_id") == col("m.movie_id"),
        "inner"
    )
    .join(
        silver_reviews.alias("r"),
        col("w.movie_id") == col("r.movie_id"),
        "left"
    )
    .groupBy(
        col("m.movie_id"),
        col("m.title"),
        col("m.genre_primary"),
        col("m.content_type")
    )
    .agg(
        count("w.session_id").alias("total_views"),
        round(sum("w.watch_duration_minutes"),2).alias("total_watch_minutes"),
        round(avg("w.progress_percentage"),2).alias("avg_completion"),
        round(avg("r.rating"),2).alias("avg_user_rating"),
        round(avg("r.sentiment_score"),3).alias("avg_sentiment")
    )
)

# COMMAND ----------

gold_content_performance.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.ott_project.gold_content_performance")

# COMMAND ----------

print(
    spark.table("workspace.ott_project.gold_content_performance").count()
)

# COMMAND ----------

display(gold_content_performance)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Q9) Where do users stop watching? What percentage of users complete the content? What is the drop-off distribution?
# MAGIC
# MAGIC User Retention / Drop-off Analytics

# COMMAND ----------

from pyspark.sql.functions import when

dropoff = silver_watch_history.withColumn(
    "dropoff_stage",
    when(col("progress_percentage") < 25, "0-25%")
    .when(col("progress_percentage") < 50, "25-50%")
    .when(col("progress_percentage") < 75, "50-75%")
    .otherwise("75-100%")
)

# COMMAND ----------

gold_retention = (
    dropoff.groupBy("dropoff_stage")
    .agg(
        count("session_id").alias("sessions"),
        countDistinct("user_id").alias("unique_users"),
        round(avg("progress_percentage"),2).alias("avg_progress"),
        round(avg("watch_duration_minutes"),2).alias("avg_watch_duration")
    )
)

# COMMAND ----------

gold_retention.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.ott_project.gold_retention")

# COMMAND ----------

print(
    spark.table("workspace.ott_project.gold_retention").count()
)

# COMMAND ----------

display(gold_retention)

# COMMAND ----------

# MAGIC %md
# MAGIC #### VIEWS

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW workspace.ott_project.v_daily_watch AS
# MAGIC
# MAGIC SELECT *
# MAGIC FROM workspace.ott_project.gold_daily_watch;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW workspace.ott_project.v_genre_popularity AS
# MAGIC
# MAGIC SELECT *
# MAGIC FROM workspace.ott_project.gold_genre;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW workspace.ott_project.v_device_analysis AS
# MAGIC
# MAGIC SELECT *
# MAGIC FROM workspace.ott_project.gold_device;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW workspace.ott_project.v_search_behaviour AS
# MAGIC
# MAGIC SELECT *
# MAGIC FROM workspace.ott_project.gold_search;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW workspace.ott_project.v_recommendation AS
# MAGIC
# MAGIC SELECT *
# MAGIC FROM workspace.ott_project.gold_recommendation;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW workspace.ott_project.v_user_behaviour AS
# MAGIC
# MAGIC SELECT *
# MAGIC FROM workspace.ott_project.gold_user;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW workspace.ott_project.v_reviews AS
# MAGIC
# MAGIC SELECT *
# MAGIC FROM workspace.ott_project.gold_reviews;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW workspace.ott_project.v_content_performance AS
# MAGIC
# MAGIC SELECT *
# MAGIC FROM workspace.ott_project.gold_content_performance;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW workspace.ott_project.v_retention AS
# MAGIC
# MAGIC SELECT *
# MAGIC FROM workspace.ott_project.gold_retention;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Top watched genres

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM workspace.ott_project.v_genre_popularity
# MAGIC ORDER BY watch_minutes DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Most preferred devices

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM workspace.ott_project.v_device_analysis
# MAGIC ORDER BY watch_time DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Average completion rate

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC AVG(avg_completion) AS completion
# MAGIC FROM workspace.ott_project.v_daily_watch;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Recommendation CTR

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM workspace.ott_project.v_recommendation
# MAGIC ORDER BY clicked DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Highest Rated Genre

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM workspace.ott_project.v_reviews
# MAGIC ORDER BY avg_rating DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Content with highest watch time

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM workspace.ott_project.v_content_performance
# MAGIC ORDER BY total_watch_minutes DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC #### User Drop-off

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM workspace.ott_project.v_retention
# MAGIC ORDER BY sessions DESC;

# COMMAND ----------

# MAGIC %%sql
# MAGIC SHOW TABLES IN workspace.ott_project;

# COMMAND ----------

spark.catalog.listTables("workspace.ott_project")

# COMMAND ----------

display(spark.table("workspace.ott_project.gold_genre"))

# COMMAND ----------

display(spark.table("workspace.ott_project.gold_content_performance"))

# COMMAND ----------

for table in [
    "gold_content_performance",
    "gold_daily_watch",
    "gold_device",
    "gold_genre",
    "gold_recommendation",
    "gold_retention",
    "gold_reviews",
    "gold_search",
    "gold_user"
]:
    print(table)
    spark.table(f"workspace.ott_project.{table}").count()

# COMMAND ----------

display(spark.table("workspace.ott_project.gold_content_performance"))

# COMMAND ----------

display(spark.table("workspace.ott_project.gold_daily_watch"))

# COMMAND ----------

display(spark.table("workspace.ott_project.gold_device"))

# COMMAND ----------

display(spark.table("workspace.ott_project.gold_genre"))

# COMMAND ----------

display(spark.table("workspace.ott_project.gold_recommendation"))

# COMMAND ----------

display(spark.table("workspace.ott_project.gold_retention"))

# COMMAND ----------

display(spark.table("workspace.ott_project.gold_reviews"))

# COMMAND ----------

display(spark.table("workspace.ott_project.gold_search"))

# COMMAND ----------

display(spark.table("workspace.ott_project.gold_user"))

# COMMAND ----------

spark.table("workspace.ott_project.gold_content_performance").printSchema()

# COMMAND ----------

spark.table("workspace.ott_project.gold_daily_watch").printSchema()

# COMMAND ----------

spark.table("workspace.ott_project.gold_device").printSchema()

# COMMAND ----------

spark.table("workspace.ott_project.gold_genre").printSchema()

# COMMAND ----------

spark.table("workspace.ott_project.gold_recommendation").printSchema()

# COMMAND ----------

spark.table("workspace.ott_project.gold_retention").printSchema()

# COMMAND ----------

spark.table("workspace.ott_project.gold_reviews").printSchema()

# COMMAND ----------

spark.table("workspace.ott_project.gold_search").printSchema()

# COMMAND ----------

spark.table("workspace.ott_project.gold_user").printSchema()

# COMMAND ----------

df = spark.table("workspace.ott_project.gold_content_performance")

df.coalesce(1) \
  .write \
  .mode("overwrite") \
  .option("header", "true") \
  .csv("/Volumes/workspace/ott_project/bronze_export/tableau/gold_content_performance")

# COMMAND ----------

dbutils.fs.ls("/Volumes/workspace/ott_project/bronze_export/tableau/gold_content_performance")

# COMMAND ----------

tables = [
    "gold_content_performance",
    "gold_daily_watch",
    "gold_device",
    "gold_genre",
    "gold_recommendation",
    "gold_retention",
    "gold_reviews",
    "gold_search",
    "gold_user"
]

for table in tables:
    try:
        df = spark.table(f"workspace.ott_project.{table}")
        df.coalesce(1) \
          .write \
          .mode("overwrite") \
          .option("header", "true") \
          .csv(f"/Volumes/workspace/ott_project/bronze_export/tableau/{table}")

        print(f"✅ Exported {table}")

    except Exception as e:
        print(f"❌ Failed {table}: {e}")

print("Export completed.")

# COMMAND ----------

display(dbutils.fs.ls("/Volumes/workspace/ott_project/bronze_export/tableau"))

# COMMAND ----------

