from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pyspark.sql import SparkSession
import os

def run_retail_etl_postgres():
    # Java setup (required in Docker)
    os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
    os.environ["PATH"] = os.environ["JAVA_HOME"] + "/bin:" + os.environ["PATH"]

    # Spark session
    spark = (
        SparkSession.builder
        .appName("Retail Data ETL Job")
        .getOrCreate()
    )

    # Input CSV path (inside container)
    input_path = "/opt/airflow/data/raw/supermarket_sales.csv"

    # Read CSV
    df = spark.read.option("header", True).csv(input_path)

    # Clean data
    cleaned_df = df.dropna().dropDuplicates()

    # PostgreSQL JDBC connection
    pg_url = "jdbc:postgresql://postgres:5432/retaildb"
    pg_table = "retail_sales"
    pg_properties = {
        "user": "postgres",
        "password": "postgres",
        "driver": "org.postgresql.Driver"
    }

    # Write to Postgres directly
    cleaned_df.write.jdbc(url=pg_url, table=pg_table, mode="append", properties=pg_properties)

    spark.stop()
    print("ETL and Postgres load completed successfully!")

# DAG definition
with DAG(
    dag_id='retail_data_pipeline_postgres_spark',
    start_date=datetime(2025, 11, 5),
    schedule_interval=None,
    catchup=False
) as dag:

    etl_postgres_task = PythonOperator(
        task_id='run_retail_etl_postgres',
        python_callable=run_retail_etl_postgres
    )
