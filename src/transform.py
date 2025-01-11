from pyspark.sql import SparkSession

# Step 1: Create a SparkSession
spark = SparkSession.builder \
    .appName("PySparkTest") \
    .getOrCreate()

# Step 2: Create a simple dataset
data = [("Alice", 28), ("Bob", 32), ("Cathy", 25)]
columns = ["Name", "Age"]

# Step 3: Create a DataFrame
df = spark.createDataFrame(data, columns)

# Step 4: Show the DataFrame
print("Original DataFrame:")
df.show()

# Step 5: Perform a transformation (filtering ages > 30)
filtered_df = df.filter(df.Age > 30)

print("Filtered DataFrame (Age > 30):")
filtered_df.show()

