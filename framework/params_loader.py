"""
Parameter Loading Module
Handles loading/saving job parameters from/to JSON files.
Provides standardized parameter access across all jobs.
"""
import os
import json
from .logging import get_logger
from .json_utils import dump_json_atomic

log = get_logger("params_loader")


def get_job_params_path(job_name, base_dir=None):
    """
    Generate standardized JSON file path for job parameters.
    
    Args:
        job_name: Name of the job
        base_dir: Base directory (defaults to project root/params)
        
    Returns:
        str: Absolute path to params JSON file
    """
    if base_dir is None:
        # Default to project_root/params/
        base_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "params")
    
    os.makedirs(base_dir, exist_ok=True)
    return os.path.join(base_dir, f"{job_name}.json")


def load_job_params_from_json(json_file_path):
    """
    Load job parameters from JSON file.
    
    Args:
        json_file_path: Path to JSON params file (or use JOB_PARAMS_FILE env var)
        
    Returns:
        dict: Job parameters with structure:
            - metadata: Job metadata (job_id, job_name, entity, etc.)
            - job_master: Job configuration
            - job_dependency: List of dependencies
            - process_control: List of process control records
            - sys_params: List of system parameters
    """
    if json_file_path is None:
        json_file_path = os.environ.get("JOB_PARAMS_FILE")
    
    if not json_file_path:
        raise RuntimeError(
            "No JSON file path provided and JOB_PARAMS_FILE environment variable not set. "
            "Run via orchestrator or set JOB_PARAMS_FILE manually."
        )
    
    if not os.path.exists(json_file_path):
        raise FileNotFoundError(f"Job parameters file not found: {json_file_path}")
    
    with open(json_file_path, 'r', encoding='utf-8') as f:
        all_params = json.load(f)
    
    log.info(f"Loaded job parameters from: {json_file_path}")
    return all_params


def save_job_params_to_json(job_params, json_file_path, pretty=False):
    """
    Save job parameters to JSON file atomically.
    
    Args:
        job_params: Dictionary of job parameters
        json_file_path: Path to save JSON file
        pretty: If True, format with indentation
    """
    os.makedirs(os.path.dirname(json_file_path), exist_ok=True)
    
    with open(json_file_path, 'w', encoding='utf-8') as f:
        if pretty:
            json.dump(job_params, f, indent=2)
        else:
            json.dump(job_params, f)
    
    log.info(f"Saved job parameters to: {json_file_path}")


def flatten_job_params(all_params):
    """
    Flatten structured job parameters into a simple dict for easier access.
    Extracts commonly used fields from metadata, job_master, and process_control.
    
    Args:
        all_params: Full structured parameters dict
        
    Returns:
        dict: Flattened parameters with common fields
    """
    metadata = all_params.get("metadata", {})
    job_master = all_params.get("job_master", {})
    process_control = all_params.get("process_control", [])
    
    # Get latest process_control record (first in list, already sorted by orchestrator)
    latest_process_control = process_control[0] if process_control else {}
    
    # Build flattened params
    params = {
        # From metadata
        "env": metadata.get("env"),
        "entity": metadata.get("entity"),
        "source_system": metadata.get("source_system"),
        "job_name": metadata.get("job_name"),
        "job_id": metadata.get("job_id"),
        
        # From job_master
        "script_path": job_master.get("script_path"),
        "comments": job_master.get("comments"),
        
        # From process_control (latest record)
        "biz_dt": latest_process_control.get("biz_dt"),
        "process_from_date": latest_process_control.get("process_from_date"),
        "process_to_date": latest_process_control.get("process_to_date"),
        "run_indicator": latest_process_control.get("run_indicator"),
        "run_id": latest_process_control.get("run_id"),
        "change_indicator": latest_process_control.get("change_indicator"),
    }
    
    # Parse context_params if available
    if job_master.get("context_params"):
        try:
            context_params = json.loads(job_master["context_params"]) if isinstance(job_master["context_params"], str) else job_master["context_params"]
            params["context_params"] = context_params
        except json.JSONDecodeError:
            log.warning("Failed to parse context_params from job_master")
    
    return params


def print_params_summary(all_params, json_file_path=None):
    """
    Print a formatted summary of job parameters.
    
    Args:
        all_params: Full structured parameters dict
        json_file_path: Optional path to display
    """
    job_dependency = all_params.get("job_dependency", [])
    process_control = all_params.get("process_control", [])
    sys_params = all_params.get("sys_params", [])
    
    print(f"\n{'='*70}")
    print(f"✓ Comprehensive Job Parameters Loaded")
    print(f"{'='*70}")
    if json_file_path:
        print(f"File: {json_file_path}")
    print(f"Tables included:")
    print(f"  - job_master: 1 record")
    print(f"  - job_dependency: {len(job_dependency)} records")
    print(f"  - process_control: {len(process_control)} records")
    print(f"  - sys_params: {len(sys_params)} records")
    print(f"{'='*70}\n")
