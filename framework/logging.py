
import logging, json, sys, os
from datetime import datetime
from pyspark.sql import SparkSession
from pathlib import Path


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        # Format timestamp with milliseconds
        ts = datetime.fromtimestamp(record.created).strftime("%Y-%m-%d %H:%M:%S")
        ts = f"{ts},{int(record.msecs):03d}"
        payload = {
            "ts": ts,
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        ctx = getattr(record, 'context', None)
        if ctx:
            payload['context'] = ctx
        return json.dumps(payload, ensure_ascii=False)


def _build_log_file_path(log_dir: str | os.PathLike | None = None,
                         prefix: str = "job",
                         when: datetime | None = None) -> Path:
    """
    Build a timestamped log file path like: <log_dir>/job_YYYY-MM-DD_HH-mm-ss.log
    - log_dir can be overridden via LOG_DIR environment variable.
    - Defaults to ./logs if not provided.
    """
    # Priority: function arg > env var > default ./logs
    if log_dir is None:
        log_dir = os.environ.get("LOG_DIR", "logs")

    when = when or datetime.now()  # local time; use datetime.utcnow() if you prefer UTC
    stamp = when.strftime("%Y-%m-%d_%H-%M-%S")

    # Ensure directory exists
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)

    return log_path / f"{prefix}_{stamp}.log"


def get_logger(name: str = "pipeline",
               level: int = logging.INFO,
               log_dir: str | os.PathLike | None = None,
               filename_prefix: str = "job") -> logging.Logger:
    """
    Creates a logger that writes:
      - to console (stdout) in JSON, and
      - to a timestamped file under <log_dir> (default ./logs), e.g., logs/job_2026-01-09_13-45-12.log
    You can override the directory by passing `log_dir` or setting ENV `LOG_DIR`.
    If LOG_FILE env var is set, all loggers will use that shared file instead of creating new ones.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False

    # If handlers already exist (e.g., repeated calls), avoid adding duplicates
    if logger.handlers:
        return logger

    formatter = JsonFormatter()

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(level)

    # File handler - use shared LOG_FILE if set, otherwise create timestamped file
    log_file_env = os.environ.get("LOG_FILE")
    if log_file_env:
        file_path = Path(log_file_env)
        # Ensure directory exists
        file_path.parent.mkdir(parents=True, exist_ok=True)
    else:
        file_path = _build_log_file_path(log_dir=log_dir, prefix=filename_prefix)
    
    file_handler = logging.FileHandler(file_path, mode="a", encoding="utf-8")
    file_handler.setFormatter(formatter)
    file_handler.setLevel(level)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    # Optional: emit where the file is
    logger.debug(json.dumps({"log_file": str(file_path)}))

    return logger


def set_spark_log_level(spark: SparkSession, level: str = "WARN") -> None:
    spark.sparkContext.setLogLevel(level)
