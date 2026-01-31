"""
View Kudu table data using PySpark.
Python 3.11 + PySpark 3.5.0
 ##usage python kudu_view.py job_master
"""
from pyspark.sql import SparkSession
import os
import sys
import pyspark

# Configure Java 17
os.environ['JAVA_HOME'] = r'C:\Program Files\Eclipse Adoptium\jdk-17.0.17.10-hotspot'
os.environ['PATH'] = os.path.join(os.environ['JAVA_HOME'], 'bin') + os.pathsep + os.environ.get('PATH', '')
os.environ['SPARK_HOME'] = os.path.dirname(pyspark.__file__)
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Get table name from command line or use default
table_name = sys.argv[1] if len(sys.argv) > 1 else "kudu_test"

spark = SparkSession.builder \
    .appName("Kudu View") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.kudu:kudu-spark3_2.12:1.17.0") \
    .config("spark.driver.extraJavaOptions", "--add-exports java.base/sun.nio.ch=ALL-UNNAMED") \
    .config("spark.executor.extraJavaOptions", "--add-exports java.base/sun.nio.ch=ALL-UNNAMED") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.sql.kudu.scanner.socketReadTimeoutMs", "30000") \
    .config("spark.ui.showConsoleProgress", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

kudu_master = "localhost:7051"

print(f"\n{'='*60}")
print(f"Table: {table_name}")
print(f"{'='*60}\n")

try:
    # Read from Kudu
    df = spark.read \
        .format("kudu") \
        .option("kudu.master", kudu_master) \
        .option("kudu.table", table_name) \
        .load()
    
    row_count = df.count()
    print(f"Total rows: {row_count}\n")
    
    if row_count > 0:
        # Show all rows in a nice table format
        df.show(row_count, truncate=False)
        
        # Show schema
        print("\nSchema:")
        df.printSchema()
    else:
        print("Table is empty.")
        
except Exception as e:
    print(f"Error reading table: {e}")
finally:
    spark.stop()
