"""
Initialize Kudu tables for the orchestration framework, dynamically.
Includes: job_master, process_control, job_dependency, sys_params, execution_log
Run only the table(s) passed via CLI; use --tables all to run everything.
# Run only job_master
python init_kudu_tables.py --tables job_master

# Run two tables
python init_kudu_tables.py --tables job_master process_control

# Run all tables (default behavior if you pass 'all')
python init_kudu_tables.py --tables all

# Override Kudu master and verify after inserts
python init_kudu_tables.py --tables job_master --kudu-master kudu-master.prod:7051 --verify

"""

import os
import sys
import time
import argparse
import pyspark
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, DateType
)

# ----------------------------
# Environment configuration
# ----------------------------
# (Keep your Java & Spark env config; allow override via system env if needed)

# Java path (update if different on another machine)
os.environ['JAVA_HOME'] = r'C:\Program Files\Eclipse Adoptium\jdk-17.0.17.10-hotspot'
os.environ['SPARK_HOME'] = os.path.dirname(pyspark.__file__)
os.environ['PATH'] = os.path.join(os.environ['JAVA_HOME'], 'bin') + os.pathsep + os.environ.get('PATH', '')

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


def build_spark():
    spark = SparkSession.builder \
        .appName("Kudu Dynamic Tables Init") \
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
    return spark


# -----------------------------------------
# Table initializers (one function per table)
# -----------------------------------------
def init_job_master(spark, kudu_master):
    print("\n1. Creating job_master table...")
    job_master_schema = StructType([
        StructField("job_id", StringType(), False),
        StructField("job_name", StringType(), False),
        StructField("context_params", StringType(), False),
        StructField("script_path", StringType(), False),
        StructField("comments", StringType(), False),
    ])
    job_master_data = [
        ("JOB_1001", "etl_lnd_gels_fpms_t_contract_master", "", "C:/MyFiles/GE/Orch/jobs", "first job"),
        ("JOB_2001", "etl_trans_fpms_contract_master", "", "C:/MyFiles/GE/Orch/jobs", "second_job"),
    ]
    df = spark.createDataFrame(job_master_data, job_master_schema)
    try:
        df.write.format("kudu") \
            .option("kudu.master", kudu_master) \
            .option("kudu.table", "job_master") \
            .option("kudu.operation", "upsert") \
            .mode("append").save()
        print(f"   ✓ Inserted {len(job_master_data)} rows into job_master")
    except Exception as e:
        print(f"   ✗ Error: {e}")


def init_process_control(spark, kudu_master):
    print("\n2. Creating process_control table...")
    current_timestamp = int(time.time() * 1000000)  # UNIXTIME_MICROS
    process_control_schema = StructType([
        StructField("entity", StringType(), False),
        StructField("source_system", StringType(), False),
        StructField("biz_dt", DateType(), False),
        StructField("process_from_date", DateType(), True),
        StructField("process_to_date", DateType(), True),
        StructField("run_indicator", StringType(), True),
        StructField("run_id", StringType(), True),
        StructField("updated_at", LongType(), True),
        StructField("change_indicator", StringType(), True),
    ])
    process_control_data = [
        ("GELS", "FPMS", date(2026, 1, 26), date(2026, 1, 1), date(2026, 1, 26), "Y", "20260131", current_timestamp, "Y"),
    ]
    df = spark.createDataFrame(process_control_data, process_control_schema)
    try:
        df.write.format("kudu") \
            .option("kudu.master", kudu_master) \
            .option("kudu.table", "process_control") \
            .option("kudu.operation", "upsert") \
            .mode("append").save()
        print(f"   ✓ Inserted {len(process_control_data)} rows into process_control")
    except Exception as e:
        print(f"   ✗ Error: {e}")


def init_job_dependency(spark, kudu_master):
    print("\n3. Creating job_dependency table...")
    job_dependency_schema = StructType([
        StructField("dependent_job_name", StringType(), False),
        StructField("dependent_job_id", StringType(), False),
        StructField("parent_job_name", StringType(), True),
        StructField("parent_job_id", StringType(), True),
    ])
    job_dependency_data = [
        ("etl_lnd_gels_fpms_t_contract_master", "JOB_1001", "etl_trans_fpms_contract_master", "JOB_2001"),
    ]
    df = spark.createDataFrame(job_dependency_data, job_dependency_schema)
    try:
        df.write.format("kudu") \
            .option("kudu.master", kudu_master) \
            .option("kudu.table", "job_dependency") \
            .option("kudu.operation", "upsert") \
            .mode("append").save()
        print(f"   ✓ Inserted {len(job_dependency_data)} rows into job_dependency")
    except Exception as e:
        print(f"   ✗ Error: {e}")


def init_sys_params(spark, kudu_master):
    print("\n4. Creating sys_params table...")
    current_timestamp = int(time.time() * 1000000)
    sys_params_schema = StructType([
        StructField("param_key", StringType(), False),
        StructField("environment", StringType(), False),
        StructField("key_name", StringType(), False),
        StructField("param_value", StringType(), True),
        StructField("updated_at", LongType(), True),
    ])
    sys_params_data = [
        ("spark.executor.memory", "DEV", "etl_lnd_gels_fpms_t_contract_master", "4g", current_timestamp),
        ("spark.executor.memory", "PROD", "etl_lnd_gels_fpms_t_contract_master", "16g", current_timestamp),
        ("spark.driver.memory", "DEV", "etl_lnd_gels_fpms_t_contract_master", "2g", current_timestamp),
        ("spark.driver.memory", "PROD", "etl_lnd_gels_fpms_t_contract_master", "8g", current_timestamp),
        ("kudu.master.address", "DEV", "etl_lnd_gels_fpms_t_contract_master", "localhost:7051", current_timestamp),
        ("kudu.master.address", "PROD", "etl_lnd_gels_fpms_t_contract_master", "kudu-master.prod:7051", current_timestamp),
    ]
    df = spark.createDataFrame(sys_params_data, sys_params_schema)
    try:
        df.write.format("kudu") \
            .option("kudu.master", kudu_master) \
            .option("kudu.table", "sys_params") \
            .option("kudu.operation", "upsert") \
            .mode("append").save()
        print(f"   ✓ Inserted {len(sys_params_data)} rows into sys_params")
    except Exception as e:
        print(f"   ✗ Error: {e}")


def init_execution_log(spark, kudu_master):
    print("\n5. Creating execution_log table...")
    base_time = int(time.time() * 1000000)
    execution_log_schema = StructType([
        StructField("job_id", StringType(), False),
        StructField("id", StringType(), True),
        StructField("run_id", StringType(), True),
        StructField("job_name", StringType(), True),
        StructField("status", StringType(), True),
        StructField("start_time", LongType(), True),
        StructField("end_time", LongType(), True),
        StructField("spark_app_id", StringType(), True),
    ])
    execution_log_data = [
        ("JOB_1001", "1","20260131","etl_lnd_gels_fpms_t_contract_master", "Started",   base_time - 3600000000, base_time - 3000000000, "app-20260126-001"),
        ("JOB_1001", "1", "20260131","etl_lnd_gels_fpms_t_contract_master", "Completed", base_time - 3600000000, base_time - 3000000000, "app-20260126-001"),
    ]
    df = spark.createDataFrame(execution_log_data, execution_log_schema)
    try:
        df.write.format("kudu") \
            .option("kudu.master", kudu_master) \
            .option("kudu.table", "execution_log") \
            .option("kudu.operation", "upsert") \
            .mode("append").save()
        print(f"   ✓ Inserted {len(execution_log_data)} rows into execution_log")
    except Exception as e:
        print(f"   ✗ Error: {e}")


# ----------------------------
# Verification utility
# ----------------------------
def verify_tables(spark, kudu_master, tables):
    print("\n" + "="*70)
    print("Verifying Selected Tables")
    print("="*70)
    for table in tables:
        try:
            df = spark.read.format("kudu") \
                .option("kudu.master", kudu_master) \
                .option("kudu.table", table).load()
            count = df.count()
            print(f"\n{table}: {count} rows")
            print("-" * 70)
            df.show(5, truncate=False)
        except Exception as e:
            print(f"\n{table}: ✗ Error - {e}")


# ----------------------------
# Main entry
# ----------------------------
def main():
    parser = argparse.ArgumentParser(description="Dynamic Kudu tables initializer")
    parser.add_argument(
        "--tables",
        nargs="+",
        required=True,
        help="Tables to initialize: job_master process_control job_dependency sys_params execution_log | or 'all'"
    )
    parser.add_argument(
        "--kudu-master",
        default="localhost:7051",
        help="Kudu master address (default: localhost:7051)"
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Verify (read & show) only the tables initialized in this run"
    )
    args = parser.parse_args()

    valid_tables = {
        "job_master": init_job_master,
        "process_control": init_process_control,
        "job_dependency": init_job_dependency,
        "sys_params": init_sys_params,
        "execution_log": init_execution_log,
    }

    # Resolve selection
    if len(args.tables) == 1 and args.tables[0].lower() == "all":
        selected = list(valid_tables.keys())
    else:
        selected = []
        for t in args.tables:
            key = t.strip().lower()
            if key not in valid_tables:
                print(f"✗ Unknown table '{t}'. Valid options: {', '.join(valid_tables.keys())} or 'all'")
                sys.exit(1)
            selected.append(key)

    print("="*70)
    print("Initializing Kudu Tables (Dynamic Selection)")
    print("="*70)
    print(f"Kudu Master: {args.kudu_master}")
    print(f"Tables selected: {', '.join(selected)}")

    spark = build_spark()

    try:
        # Run selected initializers
        for t in selected:
            valid_tables[t](spark, args.kudu_master)

        # Verify only what we initialized (if requested)
        if args.verify:
            verify_tables(spark, args.kudu_master, selected)

        print("\n" + "="*70)
        print("Initialization completed.")
        print("="*70)
        print("\nAvailable Tables Implemented:")
        print("  - job_master       (Job parameter configuration)")
        print("  - process_control  (Process scheduling control)")
        print("  - job_dependency   (Job dependency graph)")
        print("  - sys_params       (System configuration)")
        print("  - execution_log    (Job execution history)")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()