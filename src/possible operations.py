from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, FloatType

# 1. Data Cleaning and Type Conversion
df = df.withColumn("event_year", F.year(F.to_date("event_dates", "yyyy-MM-dd")))
df = df.withColumn("event_num_finishers", df["event_num_finishers"].cast(IntegerType()))
df = df.withColumn("athlete_birth_year", df["athlete_birth_year"].cast(IntegerType()))
df = df.withColumn("athlete_avg_speed", df["athlete_avg_speed"].cast(FloatType()))

# 2. Calculate age at event
df = df.withColumn("athlete_age_at_event", df["event_year"] - df["athlete_birth_year"])

# 3. Parse event distance
df = df.withColumn("event_distance_km", F.regexp_extract(df["event_distance"], r"(\d+\.?\d*)", 1).cast(FloatType()))

# 4. Window function for ranking athletes within each event
window_spec = Window.partitionBy("event_name", "event_year").orderBy(F.desc("athlete_avg_speed"))
df = df.withColumn("rank_in_event", F.rank().over(window_spec))

# 5. Calculate average speed for each age category and gender
avg_speed_by_category = df.groupBy("athlete_age_category", "athlete_gender") \
    .agg(F.avg("athlete_avg_speed").alias("avg_speed_category"))

df = df.join(avg_speed_by_category, ["athlete_age_category", "athlete_gender"])

# 6. Calculate speed difference from category average
df = df.withColumn("speed_diff_from_avg", df["athlete_avg_speed"] - df["avg_speed_category"])

# 7. Identify top performers
top_performers = df.filter(df["rank_in_event"] <= 3) \
    .groupBy("athlete_id") \
    .agg(F.count("*").alias("top_3_finishes"), 
         F.collect_list("event_name").alias("top_events"))

df = df.join(top_performers, "athlete_id", "left_outer")

# 8. Calculate cumulative events participated for each athlete
window_spec_cumulative = Window.partitionBy("athlete_id").orderBy("event_year").rowsBetween(Window.unboundedPreceding, 0)
df = df.withColumn("cumulative_events", F.count("event_name").over(window_spec_cumulative))

# 9. Identify improving athletes
window_spec_improvement = Window.partitionBy("athlete_id").orderBy("event_year")
df = df.withColumn("prev_speed", F.lag("athlete_avg_speed").over(window_spec_improvement))
df = df.withColumn("speed_improvement", df["athlete_avg_speed"] - df["prev_speed"])

# 10. Calculate country statistics
country_stats = df.groupBy("athlete_country") \
    .agg(F.countDistinct("athlete_id").alias("num_athletes"),
         F.avg("athlete_avg_speed").alias("country_avg_speed"),
         F.stddev("athlete_avg_speed").alias("country_speed_stddev"))

df = df.join(country_stats, "athlete_country")

# 11. Bin athletes into performance categories
df = df.withColumn("performance_category", 
                   F.when(df["speed_diff_from_avg"] > 2, "Exceptional")
                    .when(df["speed_diff_from_avg"].between(1, 2), "Above Average")
                    .when(df["speed_diff_from_avg"].between(-1, 1), "Average")
                    .when(df["speed_diff_from_avg"].between(-2, -1), "Below Average")
                    .otherwise("Needs Improvement"))

# 12. Calculate exponential moving average of athlete's speed
window_spec_ema = Window.partitionBy("athlete_id").orderBy("event_year")
df = df.withColumn("speed_ema", F.avg("athlete_avg_speed").over(window_spec_ema))

# 13. Identify versatile athletes (participating in different event distances)
versatile_athletes = df.groupBy("athlete_id") \
    .agg(F.countDistinct("event_distance").alias("num_different_distances"))

df = df.join(versatile_athletes, "athlete_id")

# 14. Calculate percentile rank for each athlete's speed within their age category and gender
window_spec_percentile = Window.partitionBy("athlete_age_category", "athlete_gender")
df = df.withColumn("speed_percentile", F.percent_rank().over(window_spec_percentile.orderBy(df["athlete_avg_speed"])))

# 15. Pivot table for athlete performance across years
pivot_df = df.groupBy("athlete_id").pivot("event_year").agg(F.avg("athlete_avg_speed"))

# 16. Combine all information
final_df = df.join(pivot_df, "athlete_id")

# 17. Cache the result for faster subsequent operations
final_df.cache()

# 18. Show some sample results
final_df.select("athlete_id", "event_name", "athlete_avg_speed", "rank_in_event", "speed_diff_from_avg", 
                "cumulative_events", "performance_category", "speed_percentile").show(5)
