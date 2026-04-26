from src.utils.spark_utils.spark_session import get_spark_session

# Initialize the engine
spark = get_spark_session("engine_check")

# Create some dummy  data
data = [
    ("Control", 1000, 50),
    ("Treatment", 1000, 75)
]
columns = ["group", "users", "conversions"]

# Build and show the DataFrame
df = spark.createDataFrame(data, columns)
df.show()

print("Success: The local PySpark engine is fully operational!")