# GE Data Engineering Framework - Project Overview

## Table of Contents
- [Project Purpose](#project-purpose)
- [Architecture Overview](#architecture-overview)
- [Directory Structure](#directory-structure)
- [Core Components](#core-components)
- [Execution Flow](#execution-flow)
- [Job Structure Standard](#job-structure-standard)
- [Configuration](#configuration)
- [Development Setup](#development-setup)
- [Common Operations](#common-operations)

---

## Project Purpose

A standardized PySpark ETL framework for migrating Talend jobs to cloud-ready, cluster-executable data pipelines. The framework provides:

- **Centralized orchestration** via Apache Kudu parameter management
- **Spark-submit based execution** for proper resource isolation and cluster deployment
- **Standardized job structure** with clear separation of code, SQL, and configuration
- **Reusable utilities** for I/O, logging, metrics, and configuration
- **Windows local dev + Cloudera cluster** compatibility

---

## Architecture Overview

```
┌─────────────┐
│    User     │  Runs orchestrator with CLI args
└──────┬──────┘
       │
       ▼
┌─────────────────────────────────────────┐
│  Orchestrator (run_kudu.py)             │
│  • Parse CLI arguments                  │
│  • Fetch params from Kudu tables        │
│  • Save JSON params file                │
│  • Call job_executor                    │
└──────┬──────────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────────┐
│  Framework (job_executor.py)            │
│  • Validate job directory structure     │
│  • Build spark-submit command           │
│  • Set environment variables            │
│  • Launch subprocess                    │
└──────┬──────────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────────┐
│  Spark Submit (subprocess)              │
│  • Launch Java process with Spark JARs  │
│  • Configure driver/executor resources  │
│  • Start Python worker process          │
│  • Inject pyspark.zip to PYTHONPATH     │
└──────┬──────────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────────┐
│  Job Script (ETL logic)                 │
│  • Load params from JSON                │
│  • Initialize SparkSession              │
│  • Read data (CSV/ORC/Parquet)          │
│  • Execute HQL transformations          │
│  • Write output (partitioned ORC)       │
└──────┬──────────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────────┐
│  Data Layer                             │
│  • Input: CSV/ORC files                 │
│  • SQL: HiveQL transformation scripts   │
│  • Output: Partitioned ORC files        │
└─────────────────────────────────────────┘
```

---

## Directory Structure

```
C:\MyFiles\GE\Orch\
│
├── framework/                          # Reusable framework modules
│   ├── __init__.py                     # Package initialization
│   ├── config.py                       # Spark configuration, environment setup
│   ├── io.py                           # Data I/O: read/write CSV, ORC, Parquet, exec HQL
│   ├── job_executor.py                 # Spark-submit launcher, subprocess management
│   ├── json_utils.py                   # JSON serialization utilities
│   ├── kudu_fetch_params.py            # Kudu table operations for param retrieval
│   ├── logging.py                      # Centralized logging with job context
│   ├── metrics.py                      # Job execution metrics, runtime tracking
│   ├── orchestrator.py                 # Job dependency resolution, orchestration
│   ├── params_loader.py                # Parameter loading/saving from JSON
│   └── perf.py                         # Performance monitoring utilities
│
├── orchestrators/                      # Orchestration entry points
│   ├── __init__.py
│   ├── run_kudu.py                     # Kudu-based parameter orchestrator (main entry)
│   └── built_submit.py                 # Build and submit utilities
│
├── jobs/                               # Legacy/test jobs (non-standardized)
│   ├── test_job.py                     # Test job for spark-submit validation
│   └── etl_lnd_gels_fpms_t_contract_master_old.py  # Old implementation
│
├── C:\data\projects\fdm\landing\       # Standardized job location (cluster path)
│   └── gels\fpms\core\
│       └── etl_lnd_gels_fpms_t_contract_master\
│           ├── scripts\                # Job Python scripts
│           │   └── etl_lnd_gels_fpms_t_contract_master.py
│           ├── sql\                    # HiveQL transformation scripts
│           │   ├── contract_transform.hql
│           │   └── tMap_1_out1.hql
│           └── manifest\               # Job metadata (optional)
│               └── manifest.json
│
├── params/                             # JSON parameter files (runtime)
│   └── etl_lnd_gels_fpms_t_contract_master.json
│
├── data/                               # Input data files
│   ├── customer_contract.csv
│   ├── customer_contract_fallback_lkp.csv
│   └── customer_contract.orc
│
├── out/                                # Output directory (local dev)
│   └── fdm_landing\
│       └── gels_fpms_t_contract_master\
│           └── biz_dt=20250131\
│               └── biz_status=ACTIVE\
│                   └── part-00000-*.orc
│
├── logs/                               # Application logs
│   └── orchestrator_YYYYMMDD_HHMMSS.log
│
├── docs/                               # Documentation
│   ├── architecture.drawio             # Original architecture diagram
│   └── project_architecture.drawio     # Updated flow diagram
│
├── tools/                              # Utility scripts
│   ├── init_params_kudu.py             # Initialize Kudu tables
│   ├── kudu_view.py                    # View Kudu table data
│   └── run_orchestrator.ps1            # PowerShell launcher
│
├── .gitignore                          # Git exclusions
├── requirements.txt                    # Python dependencies
└── PROJECT_OVERVIEW.md                 # This file
```

---

## Core Components

### 1. Framework Modules (`framework/`)

#### `job_executor.py` - Spark Submit Launcher
**Purpose:** Launch jobs via `spark-submit` subprocess with proper configuration

**Key Functions:**
- `execute_job_spark_submit()` - Main entry point for job execution
- `validate_job_structure()` - Validates standardized directory structure exists
- `get_job_script_path()` - Returns `script_path/job_name/scripts/job_name.py`
- `get_job_sql_dir()` - Returns `script_path/job_name/sql/`
- `get_job_base_dir()` - Returns `script_path/job_name/`
- `normalize_path()` - Handles Windows mixed path separators

**Responsibilities:**
- Build spark-submit command with configuration
- Set environment variables: `JOB_PARAMS_FILE`, `JOB_SQL_DIR`, `PYTHONPATH`
- Configure Python executable for Windows venv vs. cluster default
- Launch subprocess and capture output
- Validate job structure before execution

#### `params_loader.py` - Parameter Management
**Purpose:** Load/save job parameters from JSON files

**Key Functions:**
- `load_job_params_from_json()` - Load parameters from JSON file
- `save_job_params_to_json()` - Save parameters to JSON file
- `flatten_job_params()` - Flatten nested parameter dictionary
- `print_params_summary()` - Print formatted parameter summary

**Used By:** Jobs read `JOB_PARAMS_FILE` environment variable

#### `io.py` - Data I/O Operations
**Purpose:** Standardized data reading/writing and HQL execution

**Key Functions:**
- `read_csv(spark, path, **opts)` - Read CSV with options
- `read_orc(spark, path)` - Read ORC files
- `read_parquet(spark, path)` - Read Parquet files
- `write_orc(df, path, partitionBy=None)` - Write partitioned ORC
- `exec_hql(spark, temp_view, hql_path, logger)` - Execute HiveQL from file

**Features:**
- Automatic schema inference
- Partition support
- Temp view registration for SQL
- Error handling and logging

#### `logging.py` - Centralized Logging
**Purpose:** Consistent logging across framework and jobs

**Key Functions:**
- `get_logger(name, log_dir='logs')` - Get configured logger instance
- `setup_job_logging(job_name, log_dir='logs')` - Setup job-specific logging

**Features:**
- File + console handlers
- Timestamp-based log files
- Configurable log levels
- Job context in log messages

#### `kudu_fetch_params.py` - Kudu Operations
**Purpose:** Fetch job parameters from Apache Kudu tables

**Key Tables:**
- `job_master` - Job configuration (script_path, spark_config, etc.)
- `sys_params` - System-level parameters
- `job_dependency` - Job dependencies for orchestration

**Key Functions:**
- `fetch_job_params(job_name, entity, source_system, env)` - Fetch all params for job
- `build_param_dict()` - Build complete parameter dictionary

#### `config.py` - Spark Configuration
**Purpose:** Environment-specific Spark configuration

**Key Functions:**
- `get_spark_config()` - Get SparkSession configuration
- `setup_environment()` - Set environment variables (SPARK_HOME, JAVA_HOME, etc.)

#### `metrics.py` - Job Metrics
**Purpose:** Track job execution metrics

**Key Functions:**
- `track_job_start()` - Record job start time
- `track_job_end()` - Record job completion and duration
- `log_metrics()` - Log collected metrics

#### `orchestrator.py` - Job Orchestration
**Purpose:** Resolve job dependencies and execute in correct order

**Key Functions:**
- `resolve_dependencies()` - Build execution DAG
- `execute_jobs()` - Execute jobs respecting dependencies

---

### 2. Orchestrators (`orchestrators/`)

#### `run_kudu.py` - Main Orchestrator
**Purpose:** CLI entry point for executing jobs with Kudu-based parameters

**Usage:**
```bash
python orchestrators/run_kudu.py \
  --job_name etl_lnd_gels_fpms_t_contract_master \
  --entity GELS \
  --source_system FPMS \
  --env PROD \
  --pretty
```

**Options:**
- `--job_name` - Job identifier (required)
- `--entity` - Business entity (e.g., GELS)
- `--source_system` - Source system (e.g., FPMS)
- `--env` - Environment: DEV/QA/PROD
- `--use_spark_submit` - Use spark-submit execution (default: True)
- `--pretty` - Pretty-print JSON output

**Flow:**
1. Parse CLI arguments
2. Set environment variables (SPARK_HOME, HADOOP_HOME, JAVA_HOME)
3. Fetch parameters from Kudu
4. Save parameters to JSON file (`params/{job_name}.json`)
5. Call `job_executor.execute_job_spark_submit()`
6. Log results

---

### 3. Job Scripts (Standardized Structure)

#### Example: `etl_lnd_gels_fpms_t_contract_master.py`

**Location:** `C:\data\projects\fdm\landing\gels\fpms\core\etl_lnd_gels_fpms_t_contract_master\scripts\`

**Structure:**
```python
#!/usr/bin/env python
"""
ETL Landing: GELS FPMS Contract Master
Reads CSV, applies transformations via HQL, writes partitioned ORC
"""
import os
from pyspark.sql import SparkSession
from framework.params_loader import load_job_params_from_json
from framework.io import read_csv, read_orc, exec_hql, write_orc
from framework.logging import get_logger

def main():
    # 1. Initialize
    log = get_logger(__name__)
    params_file = os.environ.get("JOB_PARAMS_FILE")
    params = load_job_params_from_json(params_file)
    
    # 2. Get SQL directory from environment
    sql_dir = os.environ.get("JOB_SQL_DIR")
    
    # 3. Create SparkSession
    spark = SparkSession.builder.appName("etl_contract_master").getOrCreate()
    
    # 4. Read input data
    df_source = read_csv(spark, params["input_path"], header="true")
    df_lookup = read_csv(spark, params["lookup_path"], header="true")
    
    # 5. Execute transformation HQL
    df_result = exec_hql(
        spark, 
        'out1', 
        os.path.join(sql_dir, 'contract_transform.hql'), 
        log
    )
    
    # 6. Write output
    write_orc(
        df_result, 
        params["output_path"],
        partitionBy=["biz_dt", "biz_status"]
    )
    
    log.info("Job completed successfully")
    spark.stop()

if __name__ == "__main__":
    main()
```

**Environment Variables Available:**
- `JOB_PARAMS_FILE` - Path to JSON parameters
- `JOB_SQL_DIR` - Path to SQL directory
- `JOB_BASE_DIR` - Base job directory
- `PYTHONPATH` - Includes framework directory

---

## Execution Flow

### Complete End-to-End Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│ 1. USER ACTION                                                      │
│    python orchestrators/run_kudu.py --job_name <name> --env PROD   │
└────────────────────────────┬────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│ 2. ORCHESTRATOR (run_kudu.py)                                       │
│    • Parse arguments: job_name, entity, source_system, env          │
│    • Set environment: SPARK_HOME, JAVA_HOME, HADOOP_HOME            │
│    • Call kudu_fetch_params.fetch_job_params()                      │
│                                                                      │
│    Returns: {                                                       │
│      "metadata": {"job_name": "...", "entity": "..."},              │
│      "job_master": {"script_path": "...", "spark_config": {...}},   │
│      "sys_params": {...}                                            │
│    }                                                                 │
│                                                                      │
│    • Save to params/etl_lnd_gels_fpms_t_contract_master.json        │
│    • Call execute_job_spark_submit(job_params, json_file_path)      │
└────────────────────────────┬────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│ 3. JOB EXECUTOR (job_executor.py)                                   │
│    • Validate job structure:                                        │
│      - Check script_path/job_name/ exists                           │
│      - Check scripts/, sql/, manifest/ directories                  │
│      - Verify job_name.py exists                                    │
│                                                                      │
│    • Build spark-submit command:                                    │
│      cmd = [                                                        │
│        "C:\spark\bin\spark-submit.cmd",                             │
│        "--master", "local[*]",                                      │
│        "--driver-memory", "2g",                                     │
│        "--executor-memory", "2g",                                   │
│        "--conf", "spark.pyspark.python=.venv311\Scripts\python.exe",│
│        "--conf", "spark.executorEnv.PYTHONPATH=C:\MyFiles\GE\Orch", │
│        "C:\data\...\scripts\etl_lnd_gels_fpms_t_contract_master.py" │
│      ]                                                               │
│                                                                      │
│    • Set environment for subprocess:                                │
│      env["JOB_PARAMS_FILE"] = "params/etl_..._master.json"          │
│      env["JOB_SQL_DIR"] = "C:\data\...\sql"                         │
│      env["PYTHONPATH"] = "C:\MyFiles\GE\Orch"                       │
│                                                                      │
│    • Launch: subprocess.run(cmd, env=env, shell=True)               │
└────────────────────────────┬────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│ 4. SPARK SUBMIT PROCESS                                             │
│    • Java launches Spark JVM with JARs from C:\spark\jars\          │
│    • Configures driver: 2GB memory, local[*] master                 │
│    • Configures executors: 2GB memory each                          │
│    • Starts Python worker:                                          │
│      - Executable: C:\MyFiles\GE\Orch\.venv311\Scripts\python.exe   │
│      - PYTHONPATH: C:\spark\python\lib\pyspark.zip + framework path │
│    • Py4J gateway established (Python ↔ Java communication)         │
│    • Executes job script in Python process                          │
└────────────────────────────┬────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│ 5. JOB SCRIPT EXECUTION                                             │
│    • Load params: load_job_params_from_json(JOB_PARAMS_FILE)        │
│    • Initialize: spark = SparkSession.builder.getOrCreate()         │
│    • Read source: df = read_csv(spark, input_path)                  │
│    • Read lookup: df_lkp = read_csv(spark, lookup_path)             │
│    • Register views: df.createOrReplaceTempView("source")           │
│    • Execute HQL: df_result = exec_hql(spark, view, hql_path)       │
│    • Write output: write_orc(df_result, output_path, partitionBy)   │
│    • Cleanup: spark.stop()                                          │
└────────────────────────────┬────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│ 6. OUTPUT DATA                                                      │
│    C:\MyFiles\GE\Orch\out\fdm_landing\gels_fpms_t_contract_master\  │
│    └── biz_dt=20250131\                                             │
│        └── biz_status=ACTIVE\                                       │
│            └── part-00000-c000.snappy.orc                           │
│                                                                      │
│    • Partitioned by biz_dt, biz_status                              │
│    • ORC format with Snappy compression                             │
│    • Schema preserved from transformation                           │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Job Structure Standard

### Directory Layout

Every job follows this structure:

```
{script_path}/{job_name}/
├── scripts/
│   └── {job_name}.py          # Main job script (required)
├── sql/
│   ├── transform.hql          # HiveQL transformations (optional)
│   └── other_queries.hql
└── manifest/
    └── manifest.json          # Job metadata (optional)
```

### Example Configuration in Kudu

**Table: `job_master`**

| job_name | entity | source_system | script_path | spark_config |
|----------|--------|---------------|-------------|--------------|
| etl_lnd_gels_fpms_t_contract_master | GELS | FPMS | C:\data\projects\fdm\landing\gels\fpms\core | `{"master": "local[*]", "driver_memory": "2g"}` |

**Resulting Paths:**
- **Script:** `C:\data\projects\fdm\landing\gels\fpms\core\etl_lnd_gels_fpms_t_contract_master\scripts\etl_lnd_gels_fpms_t_contract_master.py`
- **SQL:** `C:\data\projects\fdm\landing\gels\fpms\core\etl_lnd_gels_fpms_t_contract_master\sql\`
- **Manifest:** `C:\data\projects\fdm\landing\gels\fpms\core\etl_lnd_gels_fpms_t_contract_master\manifest\manifest.json`

### Validation

`job_executor.py` validates:
1. Base directory exists: `{script_path}/{job_name}/`
2. Scripts directory exists: `{script_path}/{job_name}/scripts/`
3. Main script exists: `{script_path}/{job_name}/scripts/{job_name}.py`
4. SQL directory exists (warning if missing)
5. Manifest directory exists (warning if missing)

---

## Configuration

### Environment Setup

**Windows (Local Development):**
```powershell
# Set in PowerShell or via orchestrator
$env:SPARK_HOME = "C:\spark"
$env:HADOOP_HOME = "C:\hadoop"
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-17.0.17.10-hotspot"
$env:PYTHONPATH = "C:\MyFiles\GE\Orch"
```

**Linux/Cloudera (Cluster):**
```bash
export SPARK_HOME=/opt/cloudera/parcels/CDH/lib/spark
export HADOOP_HOME=/opt/cloudera/parcels/CDH/lib/hadoop
export JAVA_HOME=/usr/java/jdk1.8.0_181
export PYTHONPATH=/path/to/framework
```

### Spark Configuration

**Stored in Kudu `job_master.spark_config` (JSON):**

```json
{
  "master": "local[*]",                    // Local dev: local[*], Cluster: yarn
  "driver_memory": "2g",
  "executor_memory": "2g",
  "num_executors": 4,                      // Cluster only
  "executor_cores": 2,                     // Cluster only
  "python_executable": "/path/to/python",  // Optional: custom Python
  "additional_conf": {
    "spark.sql.shuffle.partitions": "200",
    "spark.default.parallelism": "100"
  },
  "py_files": []                           // Additional Python files to distribute
}
```

### Python Configuration

**Windows (Development):**
- Uses virtual environment: `.venv311\Scripts\python.exe`
- Automatically configured by `job_executor.py` when `os.name == 'nt'`
- Set via `--conf spark.pyspark.python=...`

**Cloudera (Production):**
- Uses cluster default Python: `/usr/bin/python3`
- No explicit configuration needed (unless using custom venv)
- For custom venv, use `spark_config.python_executable` + distribute via `--archives`

---

## Development Setup

### Prerequisites

1. **Java 17** - Adoptium Temurin JDK
2. **Apache Spark 3.5.8** - Standalone distribution
3. **Hadoop 3.3.1** - winutils.exe for Windows
4. **Python 3.11** - With virtual environment
5. **Apache Kudu** - For parameter storage

### Installation Steps

#### 1. Clone Repository
```bash
git clone https://github.com/wonderwomanjg/framework_repo.git
cd framework_repo
```

#### 2. Create Virtual Environment
```bash
python -m venv .venv311
.venv311\Scripts\activate  # Windows
source .venv311/bin/activate  # Linux
```

#### 3. Install Dependencies
```bash
pip install pandas pyarrow
# Do NOT install pyspark via pip - use standalone Spark
```

#### 4. Set Environment Variables
```powershell
# Windows (PowerShell)
$env:SPARK_HOME = "C:\spark"
$env:HADOOP_HOME = "C:\hadoop"
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-17.0.17.10-hotspot"
$env:PYTHONPATH = "C:\MyFiles\GE\Orch"
```

#### 5. Verify Setup
```bash
# Test Spark
C:\spark\bin\spark-submit.cmd --version

# Test Python
python --version  # Should show 3.11.x

# Test framework import
python -c "from framework import get_logger; print('OK')"
```

#### 6. Initialize Kudu Tables
```bash
python tools/init_params_kudu.py
```

---

## Common Operations

### Run a Job

**Basic Execution:**
```bash
python orchestrators/run_kudu.py \
  --job_name etl_lnd_gels_fpms_t_contract_master \
  --entity GELS \
  --source_system FPMS \
  --env PROD
```

**With Pretty Output:**
```bash
python orchestrators/run_kudu.py \
  --job_name etl_lnd_gels_fpms_t_contract_master \
  --entity GELS \
  --source_system FPMS \
  --env PROD \
  --pretty
```

### View Job Parameters

```bash
# View from Kudu
python tools/kudu_view.py --job_name etl_lnd_gels_fpms_t_contract_master

# View saved JSON
cat params/etl_lnd_gels_fpms_t_contract_master.json
```

### Test Spark Submit

```bash
cd jobs
C:\spark\bin\spark-submit.cmd `
  --master local[2] `
  --driver-memory 1g `
  --executor-memory 1g `
  --conf "spark.pyspark.python=C:\MyFiles\GE\Orch\.venv311\Scripts\python.exe" `
  --conf "spark.pyspark.driver.python=C:\MyFiles\GE\Orch\.venv311\Scripts\python.exe" `
  test_job.py
```

### Access Spark UI

**During Job Execution:**
- Open browser to `http://localhost:4040`
- View real-time job progress, stages, executors

**After Job Completion (History Server):**
```powershell
# Start history server
C:\spark\bin\spark-class.cmd org.apache.spark.deploy.history.HistoryServer

# Access at http://localhost:18080
```

### Create New Job

#### 1. Create Directory Structure
```bash
mkdir -p C:\data\projects\fdm\landing\my_entity\my_system\core\my_job_name\{scripts,sql,manifest}
```

#### 2. Create Job Script
```python
# scripts/my_job_name.py
import os
from pyspark.sql import SparkSession
from framework.params_loader import load_job_params_from_json
from framework.io import read_csv, write_orc
from framework.logging import get_logger

def main():
    log = get_logger(__name__)
    params = load_job_params_from_json(os.environ["JOB_PARAMS_FILE"])
    
    spark = SparkSession.builder.appName("my_job").getOrCreate()
    
    # Your ETL logic here
    df = read_csv(spark, params["input_path"])
    # ... transformations ...
    write_orc(df, params["output_path"])
    
    spark.stop()

if __name__ == "__main__":
    main()
```

#### 3. Add to Kudu
```python
# Using tools/init_params_kudu.py or manual insert
INSERT INTO job_master VALUES (
  'my_job_name',
  'MY_ENTITY',
  'MY_SYSTEM',
  'C:\data\projects\fdm\landing\my_entity\my_system\core',
  '{"master": "local[*]", "driver_memory": "2g"}'
);
```

#### 4. Run Job
```bash
python orchestrators/run_kudu.py \
  --job_name my_job_name \
  --entity MY_ENTITY \
  --source_system MY_SYSTEM \
  --env PROD
```

---

## Troubleshooting

### Common Issues

#### 1. ModuleNotFoundError: framework
**Cause:** PYTHONPATH not set for subprocess
**Fix:** Ensure `job_executor.py` sets `env["PYTHONPATH"] = project_root`

#### 2. Python worker crashed
**Cause:** Spark using wrong Python interpreter
**Fix:** Set `spark.pyspark.python` explicitly in spark-submit command

#### 3. Job structure validation failed
**Cause:** Job directory doesn't follow standard structure
**Fix:** Verify `{script_path}/{job_name}/scripts/{job_name}.py` exists

#### 4. UnsatisfiedLinkError: NativeIO
**Cause:** Missing winutils.exe on Windows
**Fix:** Download Hadoop binaries, set HADOOP_HOME

#### 5. Port 4040 already in use
**Cause:** Multiple Spark sessions running
**Fix:** Spark auto-increments to 4041, 4042, etc.

---

## Next Steps

1. **Migrate More Jobs:** Convert remaining Talend jobs to PySpark using this framework
2. **Add Unit Tests:** Create tests for framework modules
3. **CI/CD Pipeline:** Automate testing and deployment
4. **Cluster Deployment:** Test on Cloudera cluster with YARN
5. **Monitoring Dashboard:** Build job execution monitoring UI
6. **Documentation:** Add API docs for framework modules

---

## Contact & Support

- **Repository:** https://github.com/wonderwomanjg/framework_repo
- **Issues:** Submit via GitHub Issues
- **Architecture Diagram:** See `docs/project_architecture.drawio`

---

**Last Updated:** February 1, 2026  
**Framework Version:** 1.0  
**Spark Version:** 3.5.8  
**Python Version:** 3.11.1
