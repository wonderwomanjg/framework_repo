"""
Talend to PySpark Migration - Landing FPMS Contract Master
Simplified template for executing HQL components sequentially
"""
import os
from pyspark.sql import SparkSession
from framework import get_logger, set_spark_log_level, apply_profile, read_orc, write_orc
from framework.params_loader import load_job_params_from_json, flatten_job_params


def exec_hql(spark, view_name, hql_path, log):
    """
    Execute HQL file and create temp view.
    Returns the resulting DataFrame.
    """
    log.info(f"Executing HQL: {hql_path} -> creating view '{view_name}'")
    
    with open(hql_path, 'r', encoding='utf-8') as f:
        query = f.read()
    
    df = spark.sql(query)
    df.createOrReplaceTempView(view_name)
    
    log.info(f"✓ Created temp view '{view_name}' with {df.count()} rows")
    return df


def main():
    # ---- Initialize Spark Session ----
    spark = (
        SparkSession.builder
        .appName("landing_fpms_contract_master")
        .enableHiveSupport()
        .getOrCreate()
    )
    
    set_spark_log_level(spark, "WARN")
    apply_profile(spark, profile="medium")
    
    log = get_logger("landing_fpms_contract_master")
    log.info("Spark Session initialized with Hive support")

    # ---- Load Parameters ----
    job_params_file = os.environ.get("JOB_PARAMS_FILE")
    if not job_params_file:
        raise ValueError("Environment variable JOB_PARAMS_FILE not set")
    
    all_params = load_job_params_from_json(job_params_file)
    params = flatten_job_params(all_params)
    
    log.info("Loaded job parameters from JSON")
    log.info(f"  - job_dependency: {len(all_params.get('job_dependency', []))} records")
    log.info(f"  - process_control: {len(all_params.get('process_control', []))} records")
    log.info(f"  - sys_params: {len(all_params.get('sys_params', []))} records")

    # Extract key parameters
    env = params.get("env", "PROD")
    entity = params.get("entity", "gels")
    source_system = params.get("source_system", "fpms")
    table_name = params.get("table_name", "t_contract_master")
    
    # Paths
    orc_path = params.get("orc_path", r"C:\MyFiles\GE\Orch\data\customer_contract.orc")
    sql_dir = params.get("sql_dir", r"C:\MyFiles\GE\Orch\sql")
    out_path = params.get("out_path", r"C:\MyFiles\GE\Orch\out\fdm_landing\gels_fpms_t_contract_master")
    
    log.info(f"Parameters: env={env}, entity={entity}, source_system={source_system}, table_name={table_name}")
    log.info(f"Paths: orc_path={orc_path}, sql_dir={sql_dir}, out_path={out_path}")

    # ---- Read Source ORC ----
    log.info(f"Reading source ORC from: {orc_path}")
    df_source = read_orc(spark, orc_path)
    df_source.createOrReplaceTempView("source_data")
    log.info(f"✓ Loaded source data with {df_source.count()} rows")

    # ---- Execute HQL Components (Auto-generated from manifest) ----
    
    # tHiveInput_2 - Load reference data
    df_tHiveInput_2 = exec_hql(
        spark, 
        'row3', 
        os.path.join(sql_dir, 'tHiveInput_2.sql'),
        log
    )
    
    # tHiveRow_2 - Transform contracts
    df_tHiveRow_2 = exec_hql(
        spark,
        'contracts',
        os.path.join(sql_dir, 'tHiveRow_2.sql'),
        log
    )
    
    # tHiveInput_3 - Load lookup data
    df_tHiveInput_3 = exec_hql(
        spark,
        'ref_lookup',
        os.path.join(sql_dir, 'tHiveInput_3.sql'),
        log
    )
    
    # tMap_2_expired - Filter expired records
    df_tMap_2_expired = exec_hql(
        spark,
        'expired',
        os.path.join(sql_dir, 'tMap_2_expired.sql'),
        log
    )
    
    # tMap_2_active - Filter active records
    df_tMap_2_active = exec_hql(
        spark,
        'active',
        os.path.join(sql_dir, 'tMap_2_active.sql'),
        log
    )
    
    # tHiveRow_3 - Final transformation
    df_final = exec_hql(
        spark,
        'row6',
        os.path.join(sql_dir, 'tHiveRow_3.sql'),
        log
    )

    # ---- Write Output ----
    log.info(f"Writing ORC output to: {out_path}")
    
    # Get partition columns from params (default to biz_dt, biz_status)
    partition_cols = params.get("partition_cols", "biz_dt,biz_status")
    if isinstance(partition_cols, str):
        partition_cols = [col.strip() for col in partition_cols.split(",")]
    
    # Use framework's write_orc function
    write_orc(
        df=df_final,
        path=out_path,
        mode="overwrite",
        partitionBy=partition_cols,
        opts={"compression": "snappy"}
    )
    
    log.info(f"✓ ORC output written successfully")
    log.info(f"  Path: {out_path}")
    log.info(f"  Partitions: {partition_cols}")
    log.info(f"  Rows: {df_final.count()}")

    # ---- Cleanup ----
    log.info("Job completed successfully")
    spark.stop()


if __name__ == "__main__":
    main()