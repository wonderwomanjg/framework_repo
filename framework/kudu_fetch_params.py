"""
Kudu Orchestrator Framework Module
Provides common functions for fetching job parameters from Kudu tables.
"""
from pyspark.sql import functions as F
from .logging import get_logger

log = get_logger("kudu_fetch_params_functions")


def fetch_job_master(spark, kudu_master, job_name):
    """
    Fetch job configuration from job_master table by job_name.
    
    Args:
        spark: SparkSession
        kudu_master: Kudu master address
        job_name: Job name to search for
        
    Returns:
        dict: Single record with all columns from job_master
    """
    log.info(f"Fetching job_master for job_name='{job_name}'")
    
    df = (spark.read
          .format("kudu")
          .option("kudu.master", kudu_master)
          .option("kudu.table", "job_master")
          .load())
    
    df_filtered = df.filter(F.col("job_name") == job_name)
    rows = df_filtered.collect()
    
    if not rows:
        log.error(f"No job found in job_master for job_name='{job_name}'")
        raise RuntimeError(
            f"No job found in Kudu table 'job_master' for job_name: {job_name}"
        )
    
    row = rows[0]
    result = {field.name: row[field.name] for field in df_filtered.schema.fields}
    
    # Convert datetime/date types to string for JSON serialization
    for key, value in result.items():
        if hasattr(value, 'isoformat'):
            result[key] = value.isoformat()
    
    log.info(f"✓ Fetched job_master: job_id={result.get('job_id')}, job_name={result.get('job_name')}")
    return result


def fetch_job_dependency(spark, kudu_master, job_id):
    """
    Fetch ALL job dependency records where dependent_job_id matches job_id.
    
    Args:
        spark: SparkSession
        kudu_master: Kudu master address
        job_id: Job ID to search for in dependent_job_id column
        
    Returns:
        list: All matching records with all columns
    """
    log.info(f"Fetching job_dependency for dependent_job_id='{job_id}'")
    
    df = (spark.read
          .format("kudu")
          .option("kudu.master", kudu_master)
          .option("kudu.table", "job_dependency")
          .load())
    
    df_filtered = df.filter(F.col("dependent_job_id") == job_id)
    rows = df_filtered.collect()
    
    # Convert to list of dicts (all columns)
    result = []
    for row in rows:
        row_dict = {field.name: row[field.name] for field in df_filtered.schema.fields}
        # Convert datetime/date types to string
        for key, value in row_dict.items():
            if hasattr(value, 'isoformat'):
                row_dict[key] = value.isoformat()
        result.append(row_dict)
    
    log.info(f"✓ Fetched {len(result)} job_dependency record(s)")
    return result


def fetch_process_control(spark, kudu_master, entity, source_system):
    """
    Fetch ALL process control records for entity and source_system.
    
    Args:
        spark: SparkSession
        kudu_master: Kudu master address
        entity: Entity name
        source_system: Source system name
        
    Returns:
        list: All matching records with all columns
    """
    log.info(f"Fetching process_control for entity='{entity}', source_system='{source_system}'")
    
    df = (spark.read
          .format("kudu")
          .option("kudu.master", kudu_master)
          .option("kudu.table", "process_control")
          .load())
    
    df_filtered = df.filter(
        (F.col("entity") == entity) & 
        (F.col("source_system") == source_system)
    )
    
    rows = df_filtered.collect()
    
    if not rows:
        log.error(f"No process_control records found for entity='{entity}', source_system='{source_system}'")
        raise RuntimeError(
            f"No process control records found for entity='{entity}' and source_system='{source_system}'"
        )
    
    # Convert to list of dicts (all columns)
    result = []
    for row in rows:
        row_dict = {field.name: row[field.name] for field in df_filtered.schema.fields}
        # Convert datetime/date types to string
        for key, value in row_dict.items():
            if hasattr(value, 'isoformat'):
                row_dict[key] = value.isoformat()
        result.append(row_dict)
    
    log.info(f"✓ Fetched {len(result)} process_control record(s)")
    return result


def fetch_sys_params(spark, kudu_master, job_name):
    """
    Fetch ALL sys_params records where key_name matches job_name.
    
    Args:
        spark: SparkSession
        kudu_master: Kudu master address
        job_name: Job name to match against key_name column
        
    Returns:
        list: All matching records with all columns
    """
    log.info(f"Fetching sys_params for key_name='{job_name}'")
    
    df = (spark.read
          .format("kudu")
          .option("kudu.master", kudu_master)
          .option("kudu.table", "sys_params")
          .load())
    
    df_filtered = df.filter(F.col("key_name") == job_name)
    rows = df_filtered.collect()
    
    # Convert to list of dicts (all columns)
    result = []
    for row in rows:
        row_dict = {field.name: row[field.name] for field in df_filtered.schema.fields}
        # Convert datetime/date types to string
        for key, value in row_dict.items():
            if hasattr(value, 'isoformat'):
                row_dict[key] = value.isoformat()
        result.append(row_dict)
    
    log.info(f"✓ Fetched {len(result)} sys_params record(s)")
    return result


def fetch_all_job_params(spark, kudu_master, job_name, entity, source_system, env):
    """
    Comprehensive function to fetch all job-related parameters from Kudu tables.
    
    Args:
        spark: SparkSession
        kudu_master: Kudu master address
        job_name: Job name
        entity: Entity name
        source_system: Source system name
        env: Environment (PROD, UAT, DEV)
        
    Returns:
        dict: Comprehensive dictionary with metadata and all table data
    """
    log.info("="*70)
    log.info(f"Fetching comprehensive job parameters")
    log.info(f"  Job: {job_name}, Entity: {entity}, Source System: {source_system}, Env: {env}")
    log.info("="*70)
    
    # 1. Fetch job_master
    job_master = fetch_job_master(spark, kudu_master, job_name)
    job_id = job_master.get("job_id")
    
    # 2. Fetch job_dependency (by job_id)
    job_dependency = fetch_job_dependency(spark, kudu_master, job_id)
    
    # 3. Fetch process_control (by entity + source_system)
    process_control = fetch_process_control(spark, kudu_master, entity, source_system)
    
    # 4. Fetch sys_params (by job_name)
    sys_params = fetch_sys_params(spark, kudu_master, job_name)
    
    # Build comprehensive result
    result = {
        "metadata": {
            "env": env,
            "entity": entity,
            "source_system": source_system,
            "job_name": job_name,
            "job_id": job_id
        },
        "job_master": job_master,
        "job_dependency": job_dependency,
        "process_control": process_control,
        "sys_params": sys_params
    }
    
    log.info("="*70)
    log.info("✓ Successfully fetched all job parameters")
    log.info(f"  - job_master: 1 record")
    log.info(f"  - job_dependency: {len(job_dependency)} record(s)")
    log.info(f"  - process_control: {len(process_control)} record(s)")
    log.info(f"  - sys_params: {len(sys_params)} record(s)")
    log.info("="*70)
    
    return result
