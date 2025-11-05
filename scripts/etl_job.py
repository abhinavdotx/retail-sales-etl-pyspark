import os
from pyspark.sql import SparkSession

# Java setup (required inside Docker)
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
os.environ["PATH"] = os.environ["JAVA_HOME"] + "/bin:" + os.environ["PATH"]

# Spark session
spark = (
    SparkSession.builder
    .appName("Retail Data ETL Job")
    .getOrCreate()
)

print("Spark session created successfully!")

# Paths (inside container)
input_path = "/opt/airflow/data/raw/supermarket_sales.csv"
output_path = "file:///opt/airflow/data/processed"

df = spark.read.option("header", True).csv(input_path)
print("CSV file read successfully!")

cleaned_df = df.dropna().dropDuplicates()
print("Data cleaned successfully!")

cleaned_df.write.mode("overwrite").parquet(output_path)
print(f"Data written successfully to {output_path}")

spark.stop()
