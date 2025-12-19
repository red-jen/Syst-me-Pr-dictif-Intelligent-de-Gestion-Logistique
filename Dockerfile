FROM python:3.11-slim

WORKDIR /src

# Arguments for Spark
ARG SPARK_VERSION=3.5.1
ARG HADOOP_VERSION=3

# System packages: Java, build tools, networking tools (netcat), postgres client libs, SSL/ffi for some Python wheels
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-21-jre-headless \
        curl wget ca-certificates \
        netcat-traditional \
        build-essential \
        libpq-dev libssl-dev libffi-dev && \
    rm -rf /var/lib/apt/lists/*

# Download and install Spark (for spark-submit usage in Airflow or manual invocations)
RUN curl -fsSL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -o /tmp/spark.tgz && \
    tar -xzf /tmp/spark.tgz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm /tmp/spark.tgz

# Environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64 \
    SPARK_HOME=/opt/spark \
    PATH=/opt/spark/bin:$PATH \
    PYSPARK_PYTHON=python \
    PYSPARK_DRIVER_PYTHON=python \
    MODEL_DIR=/src/models \
    PIP_REQUIRE_HASHES=0 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Python dependencies
COPY requirements.txt /src/requirements.txt
RUN python -m pip install --upgrade pip && \
    pip install --default-timeout=1000 --no-cache-dir -r requirements.txt

# Copy project source
COPY . /src

# Create model directory if absent
RUN mkdir -p /src/models

# Expose common service ports (Streamlit, Jupyter, FastAPI)
EXPOSE 1234 8888 8000

# Default command (can be overridden per service in docker-compose)
CMD ["streamlit", "run", "view/app.py", "--server.port=1234", "--server.address=0.0.0.0"]
