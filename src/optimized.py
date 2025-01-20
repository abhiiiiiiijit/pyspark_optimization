from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, FloatType
import time
# Step 1: Create a SparkSession
spark = SparkSession.builder \
    .appName("PySparkTest") \
    .getOrCreate()

# Step 2: Create a simple dataset
# data = [("Alice", 28), ("Bob", 32), ("Cathy", 25)]
# columns = ["Name", "Age"]
start_time = time.time()
# Step 3: Create a DataFrame
# df = spark.createDataFrame(data, columns)
df = spark.read.csv("/home/adminabhi/gitrepo/marathon_dataset/marathon_dataset.csv", header=True, inferSchema=True)

#rename the columns

# 2. Rename Columns with a Dictionary
rename_columns = {
    "summary": "summary",
    "Year of event": "event_year",
    "Event dates": "event_dates",
    "Event name": "event_name",
    "Event distance/length": "event_distance",
    "Event number of finishers": "event_num_finishers",
    "Athlete performance": "athlete_performance",
    "Athlete club": "athlete_club",
    "Athlete country": "athlete_country",
    "Athlete year of birth": "athlete_birth_year",
    "Athlete gender": "athlete_gender",
    "Athlete age category": "athlete_age_category",
    "Athlete average speed": "athlete_avg_speed",
    "Athlete ID": "athlete_id"
}
#df = df.select([F.col(k).alias(v) for k, v in rename_columns.items()])

df = df.withColumnsRenamed(rename_columns)

#cast the columns to appropriate data types

df = df.withColumn("athlete_avg_speed", F.col("athlete_avg_speed").cast("float")) \
        .withColumn("event_num_finishers", F.col("event_num_finishers").cast("int")) 

# 2. Calculate age at event
df = df.withColumn("athlete_age_at_event", df["event_year"] - df["athlete_birth_year"])

# df.rdd.getNumPartitions()
# 3. Parse event distance
df = df.withColumn("event_distance_km", F.regexp_extract(df["event_distance"], r"(\d+\.?\d*)", 1).cast(FloatType()))

# 4. Window function for ranking athletes within each event
window_spec = Window.partitionBy("event_name", "event_year").orderBy(F.desc("athlete_avg_speed"))
df = df.withColumn("rank_in_event", F.rank().over(window_spec))

# 5. Calculate average speed for each age category and gender
avg_speed_by_category = df.groupBy("athlete_age_category", "athlete_gender") \
    .agg(F.avg("athlete_avg_speed").alias("avg_speed_category"))

df = df.join(avg_speed_by_category, ["athlete_age_category", "athlete_gender"])

df.show()
# # Step 4: Show the DataFrame
# print("Original DataFrame:")
# df.describe().show()

# # Step 5: Perform a transformation (filtering ages > 30)
# # filtered_df = df.filter(df.Age > 30)
# agg_df = df.filter(F.col('athlete_country')=='GER')\
#         .groupBy(F.col("event_year"),F.col('athlete_country'), F.col("event_name"), F.col("athlete_gender"))\
#         .agg(F.avg(F.col("athlete_avg_speed")).alias("avg_speed"), F.count(F.col("athlete_id")).alias("num_athletes"))

# print("Filtered DataFrame (Age > 30):")
# agg_df.show()

end_time = time.time()
print(f"Execution Time (before optimization): {end_time - start_time} seconds")
# Keep the SparkSession alive
# input("Press Enter to terminate the SparkSession...")
# spark.stop()

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
 
spark.stop()