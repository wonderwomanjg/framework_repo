"""
Talend to PySpark Migration - Multi-File Landing Test
Reads from CSV files, applies transformations, writes partitioned output to HDFS
Follows standardized job template pattern
"""
import os
from pyspark.sql import SparkSession
from framework import get_logger, set_spark_log_level
from framework.io import write_orc
from framework.params_loader import load_and_merge_params

def exec_hql(spark, view_name, hql_path, log, params=None):
    """Execute HQL file with parameter substitution and create temp view
    Supports both ${key} and context.key (Talend) formats
    """
    log.info(f"Executing HQL: {hql_path} -> creating view '{view_name}'")
    
    with open(hql_path, 'r', encoding='utf-8') as f:
        query = f.read()
    
    # Parameter substitution if provided
    if params:
        for key, value in params.items():
            # Replace ${key} format
            placeholder = f"${{{key}}}"
            query = query.replace(placeholder, str(value))
            
            # Replace Talend patterns - ORDER MATTERS (most specific first)
            # Pattern 1: ''" + context.key + "'' → 'value' (double single quotes)
            talend_pattern1 = f"''\" + context.{key} + \"''"
            query = query.replace(talend_pattern1, f"'{value}'")
            
            # Pattern 2: '" + context.key + "' → 'value' (single quotes)
            talend_pattern2 = f"'\" + context.{key} + \"'"
            query = query.replace(talend_pattern2, f"'{value}'")
            
            # Pattern 3: " + context.key + " → value (no quotes - for numeric columns)
            talend_pattern3 = f"\" + context.{key} + \""
            query = query.replace(talend_pattern3, str(value))
            
            # Pattern 4: Simple context.key (standalone - only if not already replaced)
            context_placeholder = f"context.{key}"
            query = query.replace(context_placeholder, str(value))
            
            log.info(f"Replaced parameter: {key} -> {value}")
    
    df = spark.sql(query)
    df.createOrReplaceTempView(view_name)
    
    count = df.count()
    log.info(f"[OK] Created temp view '{view_name}' with {count:,} rows")
    return df

def main():
    # ---- Initialize Spark Session ----
    spark = (
        SparkSession.builder
        .appName("etl_lnd_multi_file_test")
        .config("spark.sql.shuffle.partitions", "10")
        .enableHiveSupport()
        .getOrCreate()
    )
    
    set_spark_log_level(spark, "WARN")
    log = get_logger("etl_lnd_multi_file_test")
    log.info("=" * 80)
    log.info("ETL Job: Multi-File Test Landing")
    log.info("=" * 80)

    # ---- Load Parameters ----
    # Load and merge parameters: Kudu params + job-specific context params
    # Kudu params take precedence over job context params
    #params = load_and_merge_params()
# Both static file paths
    params = load_and_merge_params(
    kudu_params_file="C:/MyFiles/GE/Orch/params/my_test_job.json"
)
    
    log.info("Job Configuration:")
    log.info(f"  Parameters loaded and merged from Kudu + job contexts")

    # Extract parameters
    sql_dir = os.environ.get("JOB_SQL_DIR")
    if not sql_dir:
        raise ValueError("Environment variable JOB_SQL_DIR not set")
    
    data_dir = params.get("data_dir", r"C:/MyFiles/GE/Orch/data/test_multi_file")
    
    # Paths temporary for this POC only , in GE we will get this paths from sql components
    ################################################################################
    input_path1 = "file:///" + params.get("input_path", r"C:\MyFiles\GE\Orch\data\test_multi_file\customer_master.csv").replace("\\", "/")
    input_path2 = "file:///" + params.get("input_path", r"C:\MyFiles\GE\Orch\data\test_multi_file\portfolio_mapping.csv").replace("\\", "/")
    input_path3 = "file:///" + params.get("input_path", r"C:\MyFiles\GE\Orch\data\test_multi_file\contract_details.csv").replace("\\", "/")
    input_path4 = "file:///" + params.get("input_path", r"C:\MyFiles\GE\Orch\data\test_multi_file\product_lookup.csv").replace("\\", "/")
    input_path5 = "file:///" + params.get("input_path", r"C:\MyFiles\GE\Orch\data\test_multi_file\transaction_history.csv").replace("\\", "/")
    out_path = params.get("out_path", "hdfs://localhost:8020/data/projects/fdm/landing/test_multi_file/")    ################################################################################

  
    #########
    biz_dt = params.get("biz_dt")
    process_from_date = params.get("process_from_date",'2026-02-31')
    
    if not biz_dt:
        raise ValueError("Parameter 'biz_dt' is required")
    
    log.info(f"  SQL Directory: {sql_dir}")
    log.info(f"  Output Path: {out_path}")
    log.info(f"  Business Date: {biz_dt}")
    log.info(f"  process_from_date: {process_from_date}")

    # ===================== STAGE 1: Read from CSV Files =====================
    log.info("\n" + "=" * 80)
    log.info("STAGE 1: Reading from CSV files")
    log.info("=" * 80)
    
    hql_params = {"biz_dt": biz_dt, "process_from_date": process_from_date}
    
    # ---- Read Customer Master ----
    df_customers = spark.read.csv(input_path1,header=True,inferSchema=True)
    df_customers.createOrReplaceTempView('customers')
    
    # ---- Read Portfolio Mapping ----
    df_portfolio = spark.read.csv(input_path2,header=True,inferSchema=True)
    df_portfolio.createOrReplaceTempView('portfolio')
    
    # ---- Read Contract Details ----
    df_contracts = spark.read.csv(input_path3,header=True,inferSchema=True)
    df_contracts.createOrReplaceTempView('contracts')
    
    # ---- Read Product Lookup ----
    df_products = spark.read.csv(input_path4,header=True,inferSchema=True)
    df_products.createOrReplaceTempView('products')

    # ---- Read Transaction History ----
    df_transactions = spark.read.csv(input_path5,header=True,inferSchema=True)
    df_transactions.createOrReplaceTempView('transactions')

    # ===================== STAGE 2: Transformations =====================
    log.info("\n" + "=" * 80)
    log.info("STAGE 2: Applying transformations")
    log.info("=" * 80)
    
    # ---- tSQLRow 1: Join and Union Logic ----
    log.info("\nExecuting tSQLRow 1: Join and Union transformations...")
    exec_hql(
        spark, 
        'joined_data', 
        os.path.join(sql_dir, 'etl_lnd_gels_fpms_t_contract_master_tsqlrow_1.hql'),
        log,
        hql_params
    )
    
    # ---- tSQLRow 2: Aggregation and Filtering ----
    log.info("\nExecuting tSQLRow 2: Aggregation and filtering...")
    exec_hql(
        spark, 
        'aggregated_data', 
        os.path.join(sql_dir, 'etl_lnd_gels_fpms_t_contract_master_tsqlrow_2.hql'),
        log,
        hql_params
    )
    
    # ---- tSQLRow 3: Final Transformations ----

    exec_hql(
        spark, 
        'transformed_data', 
        os.path.join(sql_dir, 'etl_lnd_gels_fpms_t_contract_master_tsqlrow_3.hql'),
        log,
        hql_params
    )
    
    # ---- tMap: Split into Active and Expired ----
    
    df_final_active = exec_hql(
        spark, 
        'final_output_active', 
        os.path.join(sql_dir, 'etl_lnd_gels_fpms_t_contract_master_tmap_1_active.hql'),
        log,
        hql_params
    )

    log.info("\nExecuting tMap: Split by biz_status=EXPIRED...")
    df_final_expired = exec_hql(
        spark, 
        'final_output_expired', 
        os.path.join(sql_dir, 'etl_lnd_gels_fpms_t_contract_master_tmap_1_expired.hql'),
        log,
        hql_params
    )
    
    # Union both splits
    df_final = df_final_active.unionByName(df_final_expired)
    total_count = df_final.count()
    active_count = df_final_active.count()
    expired_count = df_final_expired.count()
    
    # ===================== STAGE 3: Write Output =====================
    log.info("\n" + "=" * 80)
    log.info("STAGE 3: Writing output")
    log.info("=" * 80)
    
    # ---- Write Final Output (Partitioned by biz_dt and biz_status) ----
    log.info(f"Writing final output to: {out_path}")
    
    partition_cols = ["biz_dt", "biz_status"]
    
    write_orc(
        df=df_final,
        path=out_path,
        mode="overwrite",
        partitionBy=partition_cols,
        opts={"compression": "snappy"}
    )
    
    log.info(f"[OK] Final output written successfully")
    log.info(f"  Output path: {out_path}")
    log.info(f"  Partitions: {', '.join(partition_cols)}")
    log.info(f"  Total records: {total_count:,}")
    
    # Show partition breakdown
    log.info("\nPartition breakdown:")
    log.info(f"  biz_dt={biz_dt}, biz_status=ACTIVE: {active_count:,} records")
    log.info(f"  biz_dt={biz_dt}, biz_status=EXPIRED: {expired_count:,} records")

    log.info("\n" + "=" * 80)
    log.info("[OK] ETL Job completed successfully")
    log.info("=" * 80)
    
    spark.stop()

if __name__ == "__main__":
    main()