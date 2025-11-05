# Retail Sales ETL Pipeline using PySpark, Airflow, Docker, and PostgreSQL

## Project Overview
This project demonstrates a complete ETL (Extract, Transform, Load) pipeline for retail sales data. It automates the process of cleaning, transforming, and loading data from a CSV file into a PostgreSQL database, using PySpark for processing and Airflow for orchestration. The pipeline is containerized using Docker for easy deployment.

---

## Tools & Technologies
- **Python & PySpark:** For data processing and transformations.  
- **Apache Airflow:** For workflow orchestration and scheduling ETL jobs.  
- **Docker & Docker Compose:** For containerizing Airflow and PostgreSQL.  
- **PostgreSQL:** As the target database for storing processed data.  
- **Parquet Format:** For storing cleaned, intermediate datasets.  

---

## Project Structure

C:/retail_data_pipeline
├── dags/ # Airflow DAG files
├── data/ # Raw and processed data
│ ├── raw/ # Original CSV files
│ └── processed/ # Cleaned Parquet files
├── jars/ # JDBC driver for PostgreSQL
├── scripts/ # Python ETL scripts
├── logs/ # Airflow logs
├── docker-compose.yml # Docker Compose setup
├── Dockerfile # Airflow + PySpark Docker image
└── README.md # Project documentation


---

## ETL Pipeline Steps

1. **Extract**  
   Reads raw CSV files from `data/raw` using PySpark.  

2. **Transform**  
   - Cleans the data by removing duplicates and null values.  
   - Applies basic transformations (type casting, column renaming, etc.).  

3. **Load**  
   - Writes processed data to Parquet files for storage.  
   - Loads the data directly into PostgreSQL using JDBC.  

4. **Orchestration**  
   - Airflow DAG schedules and manages the ETL tasks.  
   - Docker ensures reproducible environments for Airflow and PostgreSQL.  

---

## How to Run

1. Clone the repository:

```bash
git clone https://github.com/abhinavdotx/retail-sales-etl-pyspark.git
cd retail-sales-etl-pyspark

#2. Build and start Docker containers:
docker-compose up --build

#3. Access Airflow webserver:
http://localhost:8080

#Outcome

Raw retail sales data is cleaned and transformed.

ETL process is automated and can be scheduled with Airflow.

Processed data is loaded into PostgreSQL for analytics or reporting.

The pipeline demonstrates PySpark integration with Airflow and Docker for real-world ETL projects.

# Future Enhancements

Add data quality checks and automated alerts for failures.

Load data into BigQuery or other cloud data warehouses for analytics.

Implement incremental loads for large datasets.

