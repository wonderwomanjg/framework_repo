"""
Create Hive external tables on HDFS locations
Run after data is written to HDFS
"""
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("create_hive_tables")
    .config("spark.sql.warehouse.dir", "hdfs://localhost:8020/user/hive/warehouse")
    .enableHiveSupport()
    .getOrCreate()
)

# Create external table pointing to HDFS ORC files
create_table_ddl = """
CREATE EXTERNAL TABLE IF NOT EXISTS customer_contract_master (
    contract_id STRING,
    contract_key STRING,
    customer_name STRING,
    customer_country STRING,
    customer_city STRING,
    customer_state STRING,
    contract_value DOUBLE
)
PARTITIONED BY (
    biz_dt STRING,
    biz_status STRING
)
STORED AS ORC
LOCATION 'hdfs://localhost:8020/data/landing/gels/fpms/core/customer_contract_master'
TBLPROPERTIES ('orc.compress'='SNAPPY')
"""

print("Creating external table...")
spark.sql(create_table_ddl)
print("✓ Table created")

# Discover partitions from HDFS directory structure
print("\nRunning MSCK REPAIR TABLE...")
spark.sql("MSCK REPAIR TABLE customer_contract_master")
print("✓ Partitions discovered")

# Show partitions
print("\nPartitions:")
spark.sql("SHOW PARTITIONS customer_contract_master").show(truncate=False)

# Query the table
print("\nQuerying table:")
spark.sql("""
    SELECT biz_dt, biz_status, COUNT(*) as row_count
    FROM customer_contract_master
    GROUP BY biz_dt, biz_status
""").show()

# Show sample data
print("\nSample data:")
spark.sql("SELECT * FROM customer_contract_master LIMIT 10").show(truncate=False)

spark.stop()
