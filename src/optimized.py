from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, FloatType, StructType, StructField, StringType
import time

# Define explicit schema to avoid schema inference
schema = StructType([
    StructField("summary", StringType()),
    StructField("Year of event", IntegerType()),
    StructField("Event dates", StringType()),
    StructField("Event name", StringType()),
    StructField("Event distance/length", StringType()),
    StructField("Event number of finishers", IntegerType()),
    StructField("Athlete performance", StringType()),
    StructField("Athlete club", StringType()),
    StructField("Athlete country", StringType()),
    StructField("Athlete year of birth", IntegerType()),
    StructField("Athlete gender", StringType()),
    StructField("Athlete age category", StringType()),
    StructField("Athlete average speed", FloatType()),
    StructField("Athlete ID", StringType())
])

start_time = time.time()

# Step 1: Optimized SparkSession configuration
spark = SparkSession.builder \
    .appName("PySparkTest") \
    .master("local[8]")\
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

# Step 2: Read data with explicit schema and select/rename in one operation
df = spark.read.csv(
    "/home/adminabhi/gitrepo/marathon_dataset/marathon_dataset.csv",
    header=True,
    schema=schema
).select(
    F.col("summary").alias("summary"),
    F.col("Year of event").alias("event_year"),
    F.col("Event dates").alias("event_dates"),
    F.col("Event name").alias("event_name"),
    F.col("Event distance/length").alias("event_distance"),
    F.col("Event number of finishers").alias("event_num_finishers"),
    F.col("Athlete performance").alias("athlete_performance"),
    F.col("Athlete club").alias("athlete_club"),
    F.col("Athlete country").alias("athlete_country"),
    F.col("Athlete year of birth").alias("athlete_birth_year"),
    F.col("Athlete gender").alias("athlete_gender"),
    F.col("Athlete age category").alias("athlete_age_category"),
    F.col("Athlete average speed").alias("athlete_avg_speed"),
    F.col("Athlete ID").alias("athlete_id")
)

# Step 3: Combine transformations
df = df.withColumn("athlete_age_at_event", F.col("event_year") - F.col("athlete_birth_year")) \
       .withColumn("event_distance_km", 
                   F.regexp_extract(F.col("event_distance"), r"(\d+\.?\d*)", 1).cast(FloatType()))

# Step 4: Optimize window function
window_spec = Window.partitionBy("event_name", "event_year").orderBy(F.desc("athlete_avg_speed"))
df = df.withColumn("rank_in_event", F.rank().over(window_spec))

# Step 5: Optimize aggregation and join
# Cache the base DF as it's used multiple times
df.cache()

# Calculate average speed with efficient aggregation
avg_speed_by_category = df.groupBy("athlete_age_category", "athlete_gender") \
    .agg(F.avg("athlete_avg_speed").alias("avg_speed_category"))

# Broadcast the small aggregated DF
from pyspark.sql.functions import broadcast
df = df.join(broadcast(avg_speed_by_category), ["athlete_age_category", "athlete_gender"])

# Step 6: Efficient show with limit
df.show(5, truncate=False)

end_time = time.time()
print(f"Execution Time (after optimization): {end_time - start_time} seconds")



# Interactive command submission
print("Type 'exit' to terminate.")
while True:
    command = input(">>> ")
    if command.strip().lower() == "exit":
        break
    try:
        exec(command)  # Be cautious using exec in production
    except Exception as e:
        print(f"Error: {e}")
# Cleanup
df.unpersist()
spark.stop()