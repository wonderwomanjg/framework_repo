
# jobs/landing_fpms_contract_master.py
import os
import re
import json
from pyspark.sql import SparkSession, functions as F
from framework import get_logger, set_spark_log_level, apply_profile, timed
#from framework.io import write_orc
from framework.config import load_params  # <-- use the shared loader
from framework.json_utils import dump_json_atomic
from framework.io import write_orc

def _read_text_file(path):
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

def _run_hql_return_df(spark, sql_text: str):
    """
    Execute HQL statements separated by ';'.
    Returns the DataFrame of the **last** statement.
    """
    statements = [s.strip() for s in sql_text.split(";") if s.strip()]
    last_df = None
    for i, stmt in enumerate(statements):
        if i < len(statements) - 1:
            spark.sql(stmt)
        else:
            last_df = spark.sql(stmt)
    return last_df

def main():
    # ---- Spark session ----
    spark = (
        SparkSession.builder
        .appName("landing_fpms_contract_master_fs")
        # .master("local[*]")  # uncomment if you run locally
        .getOrCreate()
    )
    set_spark_log_level(spark, "WARN")
    apply_profile(spark, profile="medium")
    print("Spark Session created with profile 'medium'")
    print("Spark Configs:", spark.sparkContext.getConf().getAll())
    print("Spark UI available at:", spark.sparkContext.uiWebUrl)    

    log = get_logger("landing_fpms_contract_master_fs")

    # ---- Load job parameters using framework module ----
    from framework.params_loader import load_job_params_from_json, flatten_job_params
    
    all_params = load_job_params_from_json(os.environ.get("JOB_PARAMS_FILE"))
    params = flatten_job_params(all_params)
    
    # Access to full structured data if needed
    job_dependency = all_params.get("job_dependency", [])
    process_control = all_params.get("process_control", [])
    sys_params = all_params.get("sys_params", [])
    
    log.info(f"Loaded comprehensive parameters from framework")
    log.info(f"  - job_master: 1 record")
    log.info(f"  - job_dependency: {len(job_dependency)} records")
    log.info(f"  - process_control: {len(process_control)} records")
    log.info(f"  - sys_params: {len(sys_params)} records")

    # Required/commonly used params (now directly from flattened params)
    env = params.get("env", "PROD")
    biz_dt = params.get("biz_dt")  # from process_control table
    entity = params.get("entity", "gels")
    source_system = params.get("source_system", "fpms")
    table_name = params.get("table_name", "t_contract_master")
    
    # Process control fields
    process_from_date = params.get("process_from_date")
    process_to_date = params.get("process_to_date")
    run_indicator = params.get("run_indicator")
    run_id = params.get("run_id")
    

    # Paths (allow upstream to pass them; else fallback to your previous defaults)
    orc_path = params.get("orc_path", r"C:\MyFiles\GE\Orch\data\customer_contract.orc")
    hql_path = params.get("hql_path", r"C:\MyFiles\GE\Orch\sql\contract_transform.hql")
    out_path = params.get("out_path", r"C:\MyFiles\GE\Orch\out\fdm_landing\gels_fpms_t_contract_master_orc")

    # View name your HQL will reference
    view_name = params.get("source_view", f"{entity}_{source_system}_{table_name}")  # gels_fpms_t_contract_master

    log.info(
        f"Params: env={env}, orc_path={orc_path}, hql_path={hql_path}, out_path={out_path}, "
        f"biz_dt={biz_dt}, entity={entity}, source_system={source_system}, table_name={table_name}, "
        f"view_name={view_name}, process_from_date={process_from_date}, process_to_date={process_to_date}, "
        f"run_indicator={run_indicator}, run_id={run_id}"
    )

    # ---- Read source ORC ----
    src = spark.read.orc(orc_path)

    # Ensure biz_dt column exists and filter to current biz_dt
    if "biz_dt" not in src.columns:
        src = src.withColumn("biz_dt", F.lit(biz_dt))
    src = src.filter(F.col("biz_dt") == F.lit(biz_dt))

    # Add ETL timestamps (strings with millisecond precision)
    src_enriched = (
        src.withColumn("etl_start_dt", F.date_format(F.current_timestamp(), "yyyy-MM-dd HH:mm:ss.SSS"))
           .withColumn("etl_end_dt",   F.date_format(F.current_timestamp(), "yyyy-MM-dd HH:mm:ss.SSS"))
    )

    # Defaults for partition columns if missing
    if "biz_status" not in src_enriched.columns:
        src_enriched = src_enriched.withColumn("biz_status", F.lit("active"))
    if "change_indicator" not in src_enriched.columns:
        src_enriched = src_enriched.withColumn("change_indicator", F.lit("N"))

    # Optional: Add any additional validation here
    # (business_keys validation removed as it's not needed)

    # Register TEMP VIEW for HQL to consume
    src_enriched.createOrReplaceTempView(view_name)
    log.info(f"Created temp view '{view_name}' filtered for biz_dt={biz_dt}.")

    # ---- Load & run HQL ----
    hql_text = _read_text_file(hql_path)
    # Replace quoted placeholders first (preserve their surrounding quotes),
    # then replace any remaining bare placeholders. This avoids producing
    # doubled quotes when the template already wraps the placeholder.
    hql_text = (
        hql_text
        .replace("'${BIZ_DT}'", f"'{biz_dt}'")
        .replace("'${ENTITY}'", f"'{entity}'")
        .replace("'${ENV}'", f"'{env}'")
    )
    hql_text = (
        hql_text
        .replace("${VIEW_NAME}", view_name)
        .replace("${BIZ_DT}", biz_dt)
        .replace("${ENTITY}", entity)
        .replace("${ENV}", env)
    )

    # Quote parameter names when used as function arguments to avoid
    # Spark treating them as column identifiers. E.g., reverse(entity)
    # -> reverse('gels'). This targets the pattern func(param) only.
    try:
        hql_text = re.sub(r"(\b[A-Za-z_]\w*\s*\(\s*)entity(\s*\))",
                          r"\1'" + re.escape(entity) + r"'\2",
                          hql_text, flags=re.IGNORECASE)
        hql_text = re.sub(r"(\b[A-Za-z_]\w*\s*\(\s*)env(\s*\))",
                          r"\1'" + re.escape(env) + r"'\2",
                          hql_text, flags=re.IGNORECASE)
    except Exception:
        # if anything unexpected occurs, continue without stopping the job
        pass

    with timed(log, "HQL execution"):
        result_df = _run_hql_return_df(spark, hql_text)
    if result_df is None:
        raise RuntimeError("HQL did not yield a final SELECT result. Ensure the last statement is a SELECT.")

    # ---- Ensure required partition columns exist ----
    for c in ["biz_dt", "biz_status", "change_indicator"]:
        if c not in result_df.columns:
            if c == "biz_dt":
                result_df = result_df.withColumn("biz_dt", F.lit(biz_dt))
            elif c == "biz_status":
                result_df = result_df.withColumn("biz_status", F.lit("active"))
            elif c == "change_indicator":
                result_df = result_df.withColumn("change_indicator", F.lit("N"))

    # Keep only current biz_dt for writing
    result_today = result_df.filter(F.col("biz_dt") == F.lit(biz_dt))

    # Preview
    log.info("Final result (sample):")
    result_today.show(20, truncate=False)
    print(result_today.printSchema())
    log.info(f"result_today.show() --> {result_today.show(5)}")

     ### Write ORC with dynamic partition overwrite ----
    os.makedirs(out_path, exist_ok=True)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    write_orc(
        result_today,
        out_path,
       mode="overwrite",  # overwrites only matching partitions due to dynamic mode
       partitionBy=["biz_dt", "biz_status", "change_indicator"],
       opts={"compression": "snappy"}
    )

    
    # Write CSV directly (skip ORC on Windows to avoid NativeIO errors)
    ''' log.info("Writing output as CSV (Windows-compatible mode)")
    try:
        import pandas as pd
        from pyspark.sql.types import TimestampType, DateType

        os.makedirs(out_path, exist_ok=True)
        
        # Cast timestamp/date columns to string to avoid pandas datetime64 dtype issues
        safe_df = result_today
        for field in safe_df.schema.fields:
            if isinstance(field.dataType, (TimestampType, DateType)):
                safe_df = safe_df.withColumn(field.name, F.col(field.name).cast("string"))

        pdf = safe_df.toPandas()
        output_file = os.path.join(out_path, "customer_contract.csv")
        pdf.to_csv(output_file, index=False)
        log.info(f"✓ Wrote CSV output to: {output_file}")
        log.info(f"  Rows written: {len(pdf)}")
        
    except Exception as e:
        log.error(f"CSV write failed: {e}")
        raise'''

    # DO NOT stop Spark here - let the orchestrator handle it
    spark.stop()

if __name__ == "__main__":
    main()
