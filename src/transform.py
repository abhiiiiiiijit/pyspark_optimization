from pyspark.sql import SparkSession

# Step 1: Create a SparkSession
spark = SparkSession.builder \
    .appName("PySparkTest") \
    .getOrCreate()

# Step 2: Create a simple dataset
data = [("Alice", 28), ("Bob", 32), ("Cathy", 25)]
columns = ["Name", "Age"]

# Step 3: Create a DataFrame
# df = spark.createDataFrame(data, columns)
df = spark.read.csv("/home/adminabhi/gitrepo/marathon_dataset/marathon_dataset.csv", header=True, inferSchema=True)


df.rdd.getNumPartitions()

# Step 4: Show the DataFrame
print("Original DataFrame:")
df.show(5)

# Step 5: Perform a transformation (filtering ages > 30)
# filtered_df = df.filter(df.Age > 30)
filtered_df = df.filter(df['Year of event'] == 1996)

print("Filtered DataFrame (Age > 30):")
filtered_df.show(5)

