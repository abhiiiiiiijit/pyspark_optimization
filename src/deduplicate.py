from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from datetime import date

# Initialise Spark session
spark = SparkSession.builder.appName("Deduplicate").getOrCreate()

# Define schema
schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("user_name", StringType(), True),
    StructField("created_date", DateType(), True),
    StructField("email", StringType(), True)
])

# Create data
data = [
    (1, "Alice", date(2023, 5, 10), "alice@example.com"),
    (1, "Alice", date(2023, 6, 15), "alice_new@example.com"),
    (2, "Bob", date(2023, 7, 1), "bob@example.com"),
    (3, "Charlie", date(2023, 5, 20), "charlie@example.com"),
    (3, "Charlie", date(2023, 6, 25), "charlie_updated@example.com"),
    (4, "David", date(2023, 8, 5), "david@example.com")
]

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Deduplicate using window function for better efficiency
from pyspark.sql.window import Window

window_spec = Window.partitionBy("user_id").orderBy(F.desc("created_date"))
df_deduplicated = df.withColumn("rank", F.row_number().over(window_spec)) \
    .filter(F.col("rank") == 1) \
    .drop("rank")

# Show the result
df_deduplicated.show()
