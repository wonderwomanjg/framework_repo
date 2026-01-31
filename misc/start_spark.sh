#!/bin/bash
# Quick environment setup script for bash/Git Bash
# Usage: source ./misc/start_spark.sh

echo "Configuring Spark environment..."

# Set environment variables
export JAVA_HOME="/c/Program Files/Eclipse Adoptium/jdk-17.0.17.10-hotspot"
export SPARK_HOME="/c/spark"
export HADOOP_HOME="/c/hadoop"
export PYTHONPATH="/c/MyFiles/GE/Orch"

# Update PATH
export PATH="$JAVA_HOME/bin:$SPARK_HOME/bin:$HADOOP_HOME/bin:$PATH"

# Verify setup
echo ""
echo "Environment configured:"
echo "  JAVA_HOME: $JAVA_HOME"
echo "  SPARK_HOME: $SPARK_HOME"
echo "  HADOOP_HOME: $HADOOP_HOME"

# Test Java
echo ""
java -version 2>&1 | head -n 1

echo ""
echo "Ready to use PySpark!"
echo "Run: pyspark"