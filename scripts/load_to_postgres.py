# scripts/load_to_postgres.py
from pyspark.sql import SparkSession

# Database credentials
DB_USER = "postgres"
DB_PASS = "Executive@7099"   # <-- replace this with your actual PostgreSQL password
DB_NAME = "retail_db"
JDBC_URL = f"jdbc:postgresql://localhost:5432/{DB_NAME}"

# Start Spark session with PostgreSQL JDBC package
spark = (
    SparkSession.builder
    .appName("LoadToPostgres")
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0")
    .getOrCreate()
)

# Read cleaned parquet data
#df = spark.read.parquet("data/processed/cleaned_sales.parquet")
df = spark.read.parquet("C:/retail_data_pipeline/data/processed")


# Write to PostgreSQL (creates table retail_sales)
df.write.jdbc(
    url=JDBC_URL,
    table="retail_sales",
    mode="overwrite",
    properties={
        "user": DB_USER,
        "password": DB_PASS,
        "driver": "org.postgresql.Driver"
    }
)

print("Data loaded into PostgreSQL table 'retail_sales' in database 'retail_db'.")
spark.stop()
