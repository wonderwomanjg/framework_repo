"""
Talend to PySpark Migration - Landing FPMS Contract Master
"""
import os
from pyspark.sql import SparkSession
from framework import get_logger, set_spark_log_level, read_orc, write_orc
from framework.io import read_csv
from framework.params_loader import load_job_params_from_json, flatten_job_params

############   Execute HQL file and create temp view.   ##############
def exec_hql(spark, view_name, hql_path, log):

    log.info(f"Executing HQL: {hql_path} -> creating view '{view_name}'")
    
    with open(hql_path, 'r', encoding='utf-8') as f:
        query = f.read()
    
    df = spark.sql(query)
    df.createOrReplaceTempView(view_name)
    
    log.info(f"====================> Created temp view '{view_name}'")
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
    
    # Paths temporary for this POC only , in GE we will get this paths from sql components
    ################################################################################
    input_path = params.get("input_path", r"C:\MyFiles\GE\Orch\data\customer_contract_fallback.csv")
    lkp_path = params.get("lkp_path", r"C:\MyFiles\GE\Orch\data\customer_contract_fallback_lkp.csv")
    out_path = params.get("out_path", r"C:\MyFiles\GE\Orch\out\fdm_landing\gels_fpms_t_contract_master")
    ################################################################################

  
    # Get SQL directory from environment (set by job_executor)
    sql_dir = os.environ.get("JOB_SQL_DIR")
    if not sql_dir:
        # Fallback to params if not set by executor
        sql_dir = params.get("sql_dir", r"C:\MyFiles\GE\Orch\sql")

    log.info(f"Parameters: env={env}, entity={entity}, source_system={source_system}, table_name={table_name}")
    log.info(f"Paths: input_path={input_path}, sql_dir={sql_dir}, out_path={out_path}")

    # ---- Read Source File ----
    # Determine file format based on extension
    if input_path.endswith('.csv'):
        log.info(f"Reading source CSV from: {input_path}")

        ### Using framework's read_csv function
        df_source = read_csv(spark, input_path, opts={"header": "true", "inferSchema": "true"})
        lkp=read_csv(spark, lkp_path, opts={"header": "true", "inferSchema": "true"})

    
    elif input_path.endswith('.orc'):
        log.info(f"Reading source ORC from: {input_path}")
        df_source = read_orc(spark, input_path)


    df_source.createOrReplaceTempView("row1")
    lkp.createOrReplaceTempView("row2")
    log.info(f"====================Loaded source data=======================")

    # ---- Execute HQL Components (Auto-generated from manifest) ----
    
    # df_tMap_1_out1 - Load tmap logic from HQL file
    df_tMap_1_out1 = exec_hql(
        spark, 
        'out1', 
        os.path.join(sql_dir, 'etl_lnd_gels_fpms_t_contract_master_tMap_1_out1.hql'),
        log
    )
    
    # ---- FINALLY Write Output to ORC Paths----
    log.info(f"Writing ORC output to: {out_path}")
    
    # Get partition columns from params (default to biz_dt, biz_status)
    partition_cols = params.get("partition_cols", "biz_dt,biz_status")
    if isinstance(partition_cols, str):
        partition_cols = [col.strip() for col in partition_cols.split(",")]
    
    # Use framework's write_orc function
    write_orc(
        df=df_tMap_1_out1,
        path=out_path,
        mode="overwrite",
        partitionBy=partition_cols,
        opts={"compression": "snappy"}
    )
    
    log.info(f"====================ORC output written successfully=======================")
    log.info(f"  Path: {out_path}")
    log.info(f"  Partitions: {partition_cols}")
    log.info(f"  Rows: {df_tMap_1_out1.count()}")

    # ---- END ----
    log.info("Job completed successfully")
    spark.stop()


if __name__ == "__main__":
    main()