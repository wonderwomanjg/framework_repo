"""
python orchestrators/run_kudu.py \
  --job_name etl_lnd_gels_fpms_t_contract_master \
  --entity GELS \
  --source_system FPMS \
  --env PROD \
  --pretty
  
Kudu-based orchestrator - Fetches job parameters from Kudu tables and executes downstream jobs.
"""
import os
import argparse
import json
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Project root for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import pyspark

# Configure Java, Spark, and Hadoop for Windows
os.environ['JAVA_HOME'] = r'C:\Program Files\Eclipse Adoptium\jdk-17.0.17.10-hotspot'
os.environ['SPARK_HOME'] = r'C:\spark'  # Use explicit Spark installation, not pyspark package
os.environ['HADOOP_HOME'] = r'C:\hadoop'  # Required for winutils
os.environ['PATH'] = (
    os.path.join(os.environ['JAVA_HOME'], 'bin') + os.pathsep +
    os.path.join(os.environ['SPARK_HOME'], 'bin') + os.pathsep +
    os.path.join(os.environ['HADOOP_HOME'], 'bin') + os.pathsep +
    os.environ.get('PATH', '')
)
#from framework.params_kudu import read_kudu_kv
from framework.logging import get_logger
from framework.json_utils import dump_json_atomic
from framework.kudu_fetch_params import fetch_all_job_params
from framework.params_loader import save_job_params_to_json, get_job_params_path, print_params_summary
from framework.job_executor import execute_job_spark_submit

# Kudu configuration
KUDU_MASTER = os.environ.get("KUDU_MASTER", "localhost:7051")


def main():
    ap = argparse.ArgumentParser(description="Kudu-based orchestrator for job parameter management")
    ap.add_argument("--job_name", required=True, help="Job name (e.g., etl_lnd_gels_fpms_t_contract_master)")
    ap.add_argument("--entity", required=True, help="Entity name (e.g., GELS)")
    ap.add_argument("--source_system", required=True, help="Source system (e.g., FPMS)")
    ap.add_argument("--env", required=False, default="DEV", help="Environment (PROD, UAT, DEV)")
    ap.add_argument("--params_out", required=False, help="Path to write resolved job params JSON (deprecated, use dynamic path)")
    ap.add_argument("--pretty", action="store_true", help="Pretty-print JSON output")
    ap.add_argument("--kudu_master", required=False, help="Override Kudu master address")
    
    args = ap.parse_args()
    
    # Set shared log file path for all modules to use
    log_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs")
    os.makedirs(log_dir, exist_ok=True)
    from datetime import datetime
    log_timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_file_path = os.path.join(log_dir, f"job_{log_timestamp}.log")
    os.environ["LOG_FILE"] = log_file_path
    
    # Override Kudu master if provided
    if args.kudu_master:
        global KUDU_MASTER
        KUDU_MASTER = args.kudu_master
    
    # Create Spark session with Kudu support
    spark = (
        SparkSession.builder
        .appName("orchestrator_param_fetch_kudu")
        .config("spark.jars.packages", "org.apache.kudu:kudu-spark3_2.12:1.17.0")
        .config("spark.driver.extraJavaOptions", "--add-exports java.base/sun.nio.ch=ALL-UNNAMED")
        .config("spark.executor.extraJavaOptions", "--add-exports java.base/sun.nio.ch=ALL-UNNAMED")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.kudu.scanner.socketReadTimeoutMs", "30000")
        .master("local[*]")  # Change to YARN/cluster for production
        .getOrCreate()
    )
    
    spark.sparkContext.setLogLevel("ERROR")
    log = get_logger("orchestrator_kudu")
    
    log.info(f"Fetching configuration from Kudu at {KUDU_MASTER}")
    log.info(f"Job: {args.job_name}, Entity: {args.entity}, Source System: {args.source_system}, Env: {args.env}")
    
    # Fetch all job parameters using framework module
    job_params = fetch_all_job_params(
        spark=spark,
        kudu_master=KUDU_MASTER,
        job_name=args.job_name,
        entity=args.entity,
        source_system=args.source_system,
        env=args.env
    )
    
    job_id = job_params["metadata"]["job_id"]
    
    # Save job parameters to JSON file
    json_file_path = get_job_params_path(args.job_name)
    save_job_params_to_json(job_params, json_file_path, pretty=args.pretty)
    log.info(f"✓ Wrote comprehensive job parameters to: {json_file_path}")
    print_params_summary(job_params, json_file_path)
    
    # Execute downstream job
    try:
        execute_job_spark_submit(job_params, logger=log, json_file_path=json_file_path)
        log.info("Orchestration completed successfully")
        
    except Exception as e:
        log.error(f"Job execution failed: {e}", exc_info=True)
        raise
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()