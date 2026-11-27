FROM python:3.11-slim

# Install Java 21 for PySpark
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-21-jre-headless procps && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PYSPARK_PYTHON=python3
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000

# Default command runs the live dashboard; override in compose for the Spark job
CMD ["uvicorn", "src.dashboard.app:app", "--host", "0.0.0.0", "--port", "8000"]
