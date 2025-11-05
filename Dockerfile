FROM apache/airflow:2.9.0

USER root

# Install OpenJDK
RUN apt-get update && apt-get install -y openjdk-17-jdk && apt-get clean

# Set Java environment
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

USER airflow

# Install PySpark
RUN pip install --no-cache-dir pyspark
