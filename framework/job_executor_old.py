"""
Job Execution Module
Handles dynamic job script execution.
Provides standardized job invocation across orchestrators.
"""
import os
import importlib.util
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


def execute_job(job_params, logger=None, json_file_path=None):
    """
    Execute a job script dynamically.
    Sets JOB_PARAMS_FILE environment variable and runs job's main() function.
    
    Args:
        job_params: Job parameters dict
        logger: Optional logger instance (defaults to module logger)
        json_file_path: Path to JSON params file (if not already set in env)
        
    Returns:
        bool: True if job executed successfully, False otherwise
        
    Raises:
        Exception: If job execution fails
    """
    if logger is None:
        logger = log
    
    metadata = job_params.get("metadata", {})
    job_master = job_params.get("job_master", {})
    job_name = metadata.get("job_name")
    
    # Set environment variable with JSON file path (for job to read)
    if json_file_path:
        os.environ["JOB_PARAMS_FILE"] = json_file_path
        logger.info(f"✓ Set JOB_PARAMS_FILE={json_file_path}")
    
    # Check if script_path is defined
    if not job_master.get("script_path"):
        logger.info("No script_path defined in job_master, skipping job execution")
        return True
    
    # Get job script path
    script_file = get_job_script_path(job_params)
    
    # Validate script exists
    if not os.path.exists(script_file):
        logger.warning(f"Job script not found: {script_file}")
        return False
    
    # Execute job
    logger.info(f"\n{'='*70}")
    logger.info(f"Executing job script: {script_file}")
    logger.info(f"{'='*70}\n")
    
    try:
        # Import and run the job dynamically
        spec = importlib.util.spec_from_file_location(job_name, script_file)
        job_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(job_module)
        
        if hasattr(job_module, 'main'):
            job_module.main()
            logger.info(f"✓ Job '{job_name}' completed successfully")
            return True
        else:
            logger.warning(f"No main() function found in {script_file}")
            return False
            
    except Exception as e:
        logger.error(f"Job execution failed: {e}", exc_info=True)
        raise


def execute_job_safe(job_params, logger=None, json_file_path=None):
    """
    Execute a job script with exception handling.
    Same as execute_job but returns (success, error_message) instead of raising.
    
    Args:
        job_params: Job parameters dict
        logger: Optional logger instance
        json_file_path: Path to JSON params file
        
    Returns:
        tuple: (success: bool, error_message: str or None)
    """
    try:
        success = execute_job(job_params, logger, json_file_path)
        return success, None
    except Exception as e:
        return False, str(e)
