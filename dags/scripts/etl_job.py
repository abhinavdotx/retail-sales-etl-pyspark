import os
from pyspark.sql import SparkSession

# STEP 1: Hadoop + Java setup
os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-17.0.0.1"
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] += r";C:\hadoop\bin"
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

# STEP 2: Force Spark to disable native Windows IO
os.environ["HADOOP_OPTS"] = "-Djava.library.path=C:\\hadoop\\bin"
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    '--conf "spark.driver.extraJavaOptions=-Dorg.apache.hadoop.io.nativeio.NativeIO.disable=true" '
    '--conf "spark.executor.extraJavaOptions=-Dorg.apache.hadoop.io.nativeio.NativeIO.disable=true" pyspark-shell'
)

# STEP 3: Create Spark Session
spark = (
    SparkSession.builder
    .appName("Retail Data ETL Job (Windows Safe Mode)")
    .config("spark.hadoop.io.nativeio.enabled", "false")
    .config("spark.hadoop.native.lib", "false")
    .config("spark.hadoop.fs.file.impl.disable.cache", "true")
    .config("spark.driver.extraJavaOptions", "-Dorg.apache.hadoop.io.nativeio.NativeIO.disable=true")
    .config("spark.executor.extraJavaOptions", "-Dorg.apache.hadoop.io.nativeio.NativeIO.disable=true")
    .getOrCreate()
)

print("Spark session created successfully!")

# STEP 4: Define paths
input_path = r"C:\retail_data_pipeline\data\raw\supermarket_sales.csv"
output_path = r"file:///C:/retail_data_pipeline/data/processed"

# STEP 5: Read CSV file
df = spark.read.option("header", True).csv(input_path)
print("CSV file read successfully!")

# STEP 6: Clean data
cleaned_df = df.dropna().dropDuplicates()
print("🧹 Data cleaned successfully!")

# STEP 7: Write output to Parquet
cleaned_df.write.mode("overwrite").parquet(output_path)
print(f"Data written successfully to {output_path}")

spark.stop()
