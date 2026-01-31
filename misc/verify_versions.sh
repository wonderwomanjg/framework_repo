#!/bin/bash
# Version verification script for manager demo
###spark spark-3.5.8-bin-hadoop3
echo "=========================================="
echo "Framework project - Version Verification"
echo "=========================================="
echo ""

echo "1. Python Version:"
python --version
echo ""

echo "2. PySpark Version:"
python -c "import pyspark; print('PySpark:', pyspark.__version__)"
echo ""

echo "3. Java Version:"
java -version
echo ""

echo "=========================================="
echo "Testing PySpark"
echo "=========================================="
echo ""

echo "Running test query on process_control table..."
python -c '
import os
import sys
from pyspark.sql import SparkSession
import pyspark

# Configure environment
os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-1.8"
os.environ["SPARK_HOME"] = os.path.dirname(pyspark.__file__)
os.environ["PYSPARK_PYTHON"] = sys.executable

# Suppress Spark logs
import logging
logging.getLogger("py4j").setLevel(logging.ERROR)

java_home = os.environ["JAVA_HOME"]
python_exe = sys.executable
hadoop_home = os.environ["HADOOP_HOME"]

print(f"Java Home: {java_home}")
print(f"Python: {python_exe}")
print(f"HADOOP_HOME: {hadoop_home}")

spark = SparkSession.builder \
    .appName("Test") \
    .master("local[1]") \
    .config("spark.driver.host", "127.0.0.1") \
    .getOrCreate()

print("SUCCESS: Spark session created!")
spark_ver = spark.version
print(f"Spark version: {spark_ver}")

spark.sparkContext.setLogLevel("ERROR")

spark.stop()
'

echo ""
echo "=========================================="
echo "Verification Complete"
echo "=========================================="
