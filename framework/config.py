
import os, json
from typing import Optional, Dict, Any

def load_params(env_key: str = "JOB_PARAMS", json_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Load runtime parameters from:
      1) environment variable (JSON string), else
      2) JSON file path, else
      3) defaults (env variables).
    """
    if os.getenv(env_key):
        return json.loads(os.getenv(env_key))
    if json_path and os.path.exists(json_path):
        with open(json_path) as f:
            return json.load(f)
    return {
        'process_date': os.getenv('PROCESS_DATE'),
        'batch_id': os.getenv('BATCH_ID'),
        'base_path': os.getenv('BASE_PATH', '/data'),
        'env': os.getenv('ENV', 'dev'),
    }
