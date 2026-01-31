"""
Job Execution Module
Handles dynamic job script execution via spark-submit.
Provides standardized job invocation across orchestrators.
"""
import os
import subprocess
import importlib.util
import sys
from .logging import get_logger

log = get_logger("job_executor")


def get_job_script_path(job_params):
    """
    Build absolute path to job script from job parameters.
    
    Args:
        job_params: Job parameters dict (must contain metadata and job_master)
        
    Returns:
        str: Absolute path to job script
    """
    metadata = job_params.get("metadata", {})
    job_master = job_params.get("job_master", {})
    
    job_name = metadata.get("job_name")
    script_path = job_master.get("script_path")
    
    if not job_name:
        raise ValueError("job_name not found in metadata")
    
    if not script_path:
        raise ValueError("script_path not found in job_master")
    
    return os.path.join(script_path, f"{job_name}.py")


def validate_job_script(script_path):
    """
    Validate that job script exists and has main() function.
    
    Args:
        script_path: Path to job script
        
    Returns:
        tuple: (is_valid, error_message)
    """
    if not os.path.exists(script_path):
        return False, f"Job script not found: {script_path}"
    
    # Try to load module and check for main()
    try:
        spec = importlib.util.spec_from_file_location("temp_module", script_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        if not hasattr(module, 'main'):
            return False, f"No main() function found in {script_path}"
        
        return True, None
        
    except Exception as e:
        return False, f"Error loading script: {e}"


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
            # Verify the .cmd file exists
            if not os.path.exists(spark_submit_cmd):
                logger.error(f"spark-submit.cmd not found at: {spark_submit_cmd}")
                logger.info(f"Checking for spark-submit2.cmd or spark-submit.bat...")
                alt_cmd = os.path.join(spark_home, "bin", "spark-submit2.cmd")
                if os.path.exists(alt_cmd):
                    spark_submit_cmd = alt_cmd
                else:
                    raise FileNotFoundError(f"No valid spark-submit found in {spark_home}\\bin")
        else:  # Linux/Mac
            spark_submit_cmd = os.path.join(spark_home, "bin", "spark-submit")
    
    logger.info(f"Using spark-submit: {spark_submit_cmd}")
    
    # Get Spark config (defaults for local mode)
    spark_config = job_master.get("spark_config", {})
    master = spark_config.get("master", "local[*]")
    driver_memory = spark_config.get("driver_memory", "2g")
    executor_memory = spark_config.get("executor_memory", "2g")
    
    # Get project root directory (where framework package is located)
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
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
    
    # Add project root as a py-file (zip it if needed for cluster mode)
    # For local mode, we'll use PYTHONPATH instead
    if py_files:
        cmd.extend(["--py-files", ",".join(py_files)])
    
    # Add the Python script
    cmd.append(script_file)
    
    # Set environment for subprocess
    env = os.environ.copy()
    env["JOB_PARAMS_FILE"] = json_file_path
    env["PYTHONPATH"] = project_root + os.pathsep + env.get("PYTHONPATH", "")
    
    # Log execution details
    logger.info(f"\n{'='*70}")
    logger.info(f"Executing via spark-submit: {job_name}")
    logger.info(f"Script: {script_file}")
    logger.info(f"Params: {json_file_path}")
    logger.info(f"Project Root: {project_root}")
    logger.info(f"Master: {master}")
    logger.info(f"Command: {' '.join(cmd)}")
    logger.info(f"{'='*70}\n")
    
    try:
        # Run spark-submit (use shell=True on Windows for .cmd files)
        result = subprocess.run(
            cmd,
            env=env,
            capture_output=True,
            text=True,
            check=False,
            shell=(os.name == 'nt')  # Required for .cmd files on Windows
        )
        
        # Log output
        if result.stdout:
            logger.info(f"Job Output:\n{result.stdout}")
        if result.stderr:
            logger.warning(f"Job Stderr:\n{result.stderr}")
        
        # Check return code
        if result.returncode == 0:
            logger.info(f"✓ Job '{job_name}' completed successfully")
            return True
        else:
            logger.error(f"✗ Job '{job_name}' failed with exit code {result.returncode}")
            raise RuntimeError(f"Spark job failed with exit code {result.returncode}")
            
    except Exception as e:
        logger.error(f"Spark-submit execution failed: {e}", exc_info=True)
        raise


def execute_job_inprocess(job_params, logger=None, json_file_path=None):
    """
    Execute job in-process (original implementation).
    Faster for development/testing but doesn't use Spark cluster.
    """
    if logger is None:
        logger = log
    
    metadata = job_params.get("metadata", {})
    job_master = job_params.get("job_master", {})
    job_name = metadata.get("job_name")
    
    # Set environment variable
    if json_file_path:
        os.environ["JOB_PARAMS_FILE"] = json_file_path
        logger.info(f"✓ Set JOB_PARAMS_FILE={json_file_path}")
    
    # Check if script_path is defined
    if not job_master.get("script_path"):
        logger.info("No script_path defined, skipping execution")
        return True
    
    # Get job script path
    script_file = get_job_script_path(job_params)
    
    if not os.path.exists(script_file):
        logger.warning(f"Job script not found: {script_file}")
        return False
    
    logger.info(f"\n{'='*70}")
    logger.info(f"Executing in-process: {script_file}")
    logger.info(f"{'='*70}\n")
    
    try:
        # Import and run
        spec = importlib.util.spec_from_file_location(job_name, script_file)
        job_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(job_module)
        
        if hasattr(job_module, 'main'):
            job_module.main()
            logger.info(f"✓ Job '{job_name}' completed")
            return True
        else:
            logger.warning(f"No main() function in {script_file}")
            return False
            
    except Exception as e:
        logger.error(f"Job execution failed: {e}", exc_info=True)
        raise


def execute_job(job_params, logger=None, json_file_path=None, use_spark_submit=None):
    """
    Execute a job script.
    
    Args:
        use_spark_submit: True=spark-submit, False=in-process, None=auto-detect
    """
    if use_spark_submit is None:
        job_master = job_params.get("job_master", {})
        use_spark_submit = job_master.get("use_spark_submit", False)
    
    if use_spark_submit:
        return execute_job_spark_submit(job_params, logger, json_file_path)
    else:
        return execute_job_inprocess(job_params, logger, json_file_path)


def execute_job_safe(job_params, logger=None, json_file_path=None, use_spark_submit=None):
    """
    Execute job with exception handling.
    Returns (success, error_message) tuple.
    """
    try:
        success = execute_job(job_params, logger, json_file_path, use_spark_submit)
        return success, None
    except Exception as e:
        return False, str(e)