from pyspark.sql import SparkSession
from pyspark.sql.functions import col,avg, count
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

df = df.withColumnRenamed("summary", "summary") \
       .withColumnRenamed("Year of event", "event_year") \
       .withColumnRenamed("Event dates", "event_dates") \
       .withColumnRenamed("Event name", "event_name") \
       .withColumnRenamed("Event distance/length", "event_distance") \
       .withColumnRenamed("Event number of finishers", "event_num_finishers") \
       .withColumnRenamed("Athlete performance", "athlete_performance") \
       .withColumnRenamed("Athlete club", "athlete_club") \
       .withColumnRenamed("Athlete country", "athlete_country") \
       .withColumnRenamed("Athlete year of birth", "athlete_birth_year") \
       .withColumnRenamed("Athlete gender", "athlete_gender") \
       .withColumnRenamed("Athlete age category", "athlete_age_category") \
       .withColumnRenamed("Athlete average speed", "athlete_avg_speed") \
       .withColumnRenamed("Athlete ID", "athlete_id")

#cast the columns to appropriate data types

df = df.withColumn("athlete_avg_speed", col("athlete_avg_speed").cast("float")) \
        .withColumn("event_num_finishers", col("event_num_finishers").cast("int")) 



# df.rdd.getNumPartitions()

# # Step 4: Show the DataFrame
# print("Original DataFrame:")
# df.describe().show()

# # Step 5: Perform a transformation (filtering ages > 30)
# # filtered_df = df.filter(df.Age > 30)
agg_df = df.groupBy(col("event_year"), col("event_name")).agg(
        avg(col("athlete_avg_speed")).alias("avg_speed"),
        count(col("athlete_id")).alias("num_athletes")
        )

# print("Filtered DataFrame (Age > 30):")
agg_df.show(5)

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