"""
Job Execution Module
Spark-submit based job execution with standardized directory structure.
"""
import os
import subprocess
import importlib.util
import sys
from .logging import get_logger

log = get_logger("job_executor")

def normalize_path(path):
    """
    Normalize path to use OS-specific separators.
    Handles mixed forward/backward slashes on Windows.
    """
    if not path:
        return path
    
    # On Windows, handle mixed separators (\\, \, /)
    if os.name == 'nt':
        # First replace all backslashes (single or double) with forward slashes
        # This creates a consistent intermediate representation
        import re
        path = re.sub(r'\\+', '/', path)  # Replace one or more backslashes with /
        # Then replace all forward slashes with OS separator
        path = path.replace('/', os.sep)
    
    # Normalize the path (resolves .., . and removes duplicate separators)
    path = os.path.normpath(path)
    
    return path


def get_job_script_path(job_params):
    """Follows standardized structure: script_path/job_name/scripts/job_name.py
    """
    metadata = job_params.get("metadata", {})
    job_master = job_params.get("job_master", {})
    
    job_name = metadata.get("job_name")
    script_path = job_master.get("script_path")
    
    if not job_name:
        raise ValueError("job_name not found in metadata")
    
    if not script_path:
        raise ValueError("script_path not found in job_master")
    
    #### Replace forward slashes with OS separator
    script_path = normalize_path(script_path)

    # New standardized structure: script_path/job_name/scripts/job_name.py
    job_script = os.path.join(script_path, job_name, "scripts", f"{job_name}.py")
    
    return job_script


def get_job_sql_dir(job_params):
    """Structure: script_path/job_name/sql/
    """
    metadata = job_params.get("metadata", {})
    job_master = job_params.get("job_master", {})
    
    job_name = metadata.get("job_name")
    script_path = job_master.get("script_path")
    
    if not job_name or not script_path:
        raise ValueError("job_name and script_path required")
    
    script_path = normalize_path(script_path)
    return os.path.join(script_path, job_name, "sql")


def get_job_manifest_path(job_params):
    """Structure: script_path/job_name/manifest/manifest.json
    """
    metadata = job_params.get("metadata", {})
    job_master = job_params.get("job_master", {})
    
    job_name = metadata.get("job_name")
    script_path = job_master.get("script_path")
    
    if not job_name or not script_path:
        raise ValueError("job_name and script_path required")
    
    script_path = normalize_path(script_path)
    return os.path.join(script_path, job_name, "manifest", "manifest.json")


def get_job_base_dir(job_params):
    """    Structure: script_path/job_name/ """
    metadata = job_params.get("metadata", {})
    job_master = job_params.get("job_master", {})
    
    job_name = metadata.get("job_name")
    script_path = job_master.get("script_path")
    
    if not job_name or not script_path:
        raise ValueError("job_name and script_path required")
    
    script_path = normalize_path(script_path)
    return os.path.join(script_path, job_name)


def validate_job_structure(job_params):
    """Validate that job follows standardized directory structure.
    """
    errors = []
    
    try:
        # Get raw script_path for debugging
        job_master = job_params.get("job_master", {})
        raw_script_path = job_master.get("script_path", "")
        log.info(f"Raw script_path from Kudu: '{raw_script_path}'")
        log.info(f"After normalization: '{normalize_path(raw_script_path)}'")
        
        base_dir = get_job_base_dir(job_params)
        script_path = get_job_script_path(job_params)
        sql_dir = get_job_sql_dir(job_params)
        manifest_path = get_job_manifest_path(job_params)
        
        log.info(f"Expected base_dir: {base_dir}")
        log.info(f"Checking if exists: {os.path.exists(base_dir)}")
        
        # List parent directory to help debug
        parent_dir = os.path.dirname(base_dir)
        if os.path.exists(parent_dir):
            try:
                contents = os.listdir(parent_dir)
                log.info(f"Parent dir ({parent_dir}) contains: {contents}")
            except Exception as e:
                log.warning(f"Could not list parent: {e}")
        else:
            log.error(f"Parent directory doesn't exist: {parent_dir}")
        
        # Check base directory exists
        if not os.path.exists(base_dir):
            errors.append(f"Job base directory not found: {base_dir}")
            return False, errors
        
        # Check scripts directory
        scripts_dir = os.path.dirname(script_path)
        if not os.path.exists(scripts_dir):
            errors.append(f"Scripts directory not found: {scripts_dir}")
        
        # Check main script
        if not os.path.exists(script_path):
            errors.append(f"Job script not found: {script_path}")
        
        # Check sql directory (warning only, not all jobs need SQL)
        if not os.path.exists(sql_dir):
            log.warning(f"SQL directory not found: {sql_dir} (may not be required)")
        
        # Check manifest directory
        manifest_dir = os.path.dirname(manifest_path)
        if not os.path.exists(manifest_dir):
            errors.append(f"Manifest directory not found: {manifest_dir}")
        
        return len(errors) == 0, errors
        
    except Exception as e:
        errors.append(f"Error validating structure: {e}")
        return False, errors

def execute_job_spark_submit(job_params, logger=None, json_file_path=None):
    """
    Execute a job script via spark-submit.
    Works on both Windows (local) and Linux (cluster).
    
    Args:
        job_params: Job parameters dict
        logger: Optional logger instance
        json_file_path: Path to JSON params file
        
    Returns:
        bool: True if job executed successfully
    """
    if logger is None:
        logger = log
    
    metadata = job_params.get("metadata", {})
    job_master = job_params.get("job_master", {})
    job_name = metadata.get("job_name")
    
    # Check if script_path is defined
    if not job_master.get("script_path"):
        logger.info("No script_path defined in job_master, skipping job execution")
        return True
    
    # Validate job structure
    is_valid, errors = validate_job_structure(job_params)
    if not is_valid:
        logger.error("Job structure validation failed:")
        for error in errors:
            logger.error(f"  - {error}")
        return False
    
    # Get job script path
    script_file = get_job_script_path(job_params)
    
    if not os.path.exists(script_file):
        logger.warning(f"Job script not found: {script_file}")
        return False
    
    if not json_file_path:
        raise ValueError("json_file_path is required for spark-submit")
    
    # Determine spark-submit executable (Windows vs Linux)
    spark_home = os.environ.get("SPARK_HOME")
    
    if not spark_home:
        logger.warning("SPARK_HOME not set, using 'spark-submit' from PATH")
        spark_submit_cmd = "spark-submit.cmd" if os.name == 'nt' else "spark-submit"
    else:
        if os.name == 'nt':  # Windows
            spark_submit_cmd = os.path.join(spark_home, "bin", "spark-submit.cmd")
            if not os.path.exists(spark_submit_cmd):
                logger.error(f"spark-submit.cmd not found at: {spark_submit_cmd}")
                raise FileNotFoundError(f"No valid spark-submit found in {spark_home}\\bin")
        else:  # Linux/Mac
            spark_submit_cmd = os.path.join(spark_home, "bin", "spark-submit")
    
    logger.info(f"Using spark-submit: {spark_submit_cmd}")
    
    # Get Spark config (defaults for local mode)
    spark_config = job_master.get("spark_config", {})
    master = spark_config.get("master", "local[*]")
    driver_memory = spark_config.get("driver_memory", "2g")
    executor_memory = spark_config.get("executor_memory", "2g")

    print(f"Master: {master}, Driver Memory: {driver_memory}, Executor Memory: {executor_memory}")
    
    # Get project root directory (where framework package is located)
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    # Get job base directory (for SQL files, etc.)
    job_base_dir = get_job_base_dir(job_params)
    sql_dir = get_job_sql_dir(job_params)
    
    # Build spark-submit command
    cmd = [
        spark_submit_cmd,
        "--master", master,
        "--driver-memory", driver_memory,
        "--executor-memory", executor_memory,
        "--conf", f"spark.app.name={job_name}",
        "--conf", "spark.driver.extraJavaOptions=--add-exports java.base/sun.nio.ch=ALL-UNNAMED",
        "--conf", "spark.executor.extraJavaOptions=--add-exports java.base/sun.nio.ch=ALL-UNNAMED",
        "--conf", f"spark.executorEnv.PYTHONPATH={project_root}",
    ]
    
    # Add additional configs
    for key, value in spark_config.get("additional_conf", {}).items():
        cmd.extend(["--conf", f"{key}={value}"])
    
    # Add py-files to include framework package
    py_files = spark_config.get("py_files", [])
    if isinstance(py_files, str):
        py_files = [py_files]
    
    if py_files:
        cmd.extend(["--py-files", ",".join(py_files)])
    
    # Add the Python script
    cmd.append(script_file)
    
    # Set environment for subprocess
    env = os.environ.copy()
    env["JOB_PARAMS_FILE"] = json_file_path
    env["PYTHONPATH"] = project_root + os.pathsep + env.get("PYTHONPATH", "")
    env["JOB_BASE_DIR"] = job_base_dir  # Job can use this to find sql/, manifest/, etc.
    env["JOB_SQL_DIR"] = sql_dir
    
    # Log execution details
    logger.info(f"\n{'='*70}")
    logger.info(f"Executing via spark-submit: {job_name}")
    logger.info(f"Script: {script_file}")
    logger.info(f"Params: {json_file_path}")
    logger.info(f"Project Root: {project_root}")
    logger.info(f"Job Base Dir: {job_base_dir}")
    logger.info(f"SQL Dir: {sql_dir}")
    logger.info(f"Master: {master}")
    logger.info(f"{'='*70}\n")
    
    try:
        # Run spark-submit
        result = subprocess.run(
            cmd,
            env=env,
            capture_output=True,
            text=True,
            check=False,
            shell=(os.name == 'nt')
        )
        
        # Log output
        if result.stdout:
            logger.info(f"Job Output:\n{result.stdout}")
        if result.stderr:
            logger.warning(f"Job Stderr:\n{result.stderr}")
        
        # Check return code
        if result.returncode == 0:
            logger.info(f"Job '{job_name}' completed successfully")
            return True
        else:
            logger.error(f"Job '{job_name}' failed with exit code {result.returncode}")
            raise RuntimeError(f"Spark job failed with exit code {result.returncode}")
            
    except Exception as e:
        logger.error(f"Spark-submit execution failed: {e}", exc_info=True)
        raise
