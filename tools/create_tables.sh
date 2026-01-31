#!/bin/bash
# Create all orchestration framework tables in Kudu
# Drops and recreates tables if they already exist

MASTER="kudu-master:7051"

echo "======================================================================"
echo "Creating Orchestration Framework Tables in Kudu"
echo "======================================================================"

# Function to delete table if exists
delete_table_if_exists() {
    local table_name=$1
    echo "   Checking if table '$table_name' exists..."
    docker exec kudu-master kudu table list $MASTER 2>/dev/null | grep -q "^$table_name$"
    if [ $? -eq 0 ]; then
        echo "   ℹ Table '$table_name' exists, deleting..."
        docker exec kudu-master kudu table delete $MASTER $table_name -drop_data=true
        [ $? -eq 0 ] && echo "   ✓ Deleted table '$table_name'" || echo "   ✗ Failed to delete"
    else
        echo "   ℹ Table '$table_name' does not exist, will create new"
    fi
}

# ============================================================================
# Table 1: job_master
# ============================================================================
echo ""
echo "1. Creating job_master Table..."
delete_table_if_exists "job_master"

docker exec kudu-master kudu table create $MASTER '{
  "table_name": "job_master",
  "schema": {
    "columns": [
      {"column_name": "job_id", "column_type": "STRING", "is_nullable": false},
      {"column_name": "job_name", "column_type": "STRING", "is_nullable": false},
      {"column_name": "context_params", "column_type": "STRING", "is_nullable": true},
      {"column_name": "script_path", "column_type": "STRING", "is_nullable": true},
      {"column_name": "comments", "column_type": "STRING", "is_nullable": true}
    ],
    "key_column_names": ["job_id", "job_name"]
  },
  "partition": {
    "hash_partitions": [{"columns": ["job_id"], "num_buckets": 3}]
  },
  "num_replicas": 1
}'
[ $? -eq 0 ] && echo "   ✓ job_master Table created" || echo "   ✗ Failed"

# ============================================================================
# Table 2: process_control
# ============================================================================
echo ""
echo "2. Creating process_control Table..."
delete_table_if_exists "process_control"

docker exec kudu-master kudu table create $MASTER '{
  "table_name": "process_control",
  "schema": {
    "columns": [
      {"column_name": "entity", "column_type": "STRING", "is_nullable": false},
      {"column_name": "source_system", "column_type": "STRING", "is_nullable": false},
      {"column_name": "biz_dt", "column_type": "DATE", "is_nullable": false},
      {"column_name": "process_from_date", "column_type": "DATE", "is_nullable": true},
      {"column_name": "process_to_date", "column_type": "DATE", "is_nullable": true},
      {"column_name": "run_indicator", "column_type": "STRING", "is_nullable": true},
      {"column_name": "run_id", "column_type": "STRING", "is_nullable": true},
      {"column_name": "updated_at", "column_type": "UNIXTIME_MICROS", "is_nullable": true},
      {"column_name": "change_indicator", "column_type": "STRING", "is_nullable": true}
    ],
    "key_column_names": ["entity", "source_system", "biz_dt"]
  },
  "partition": {
    "hash_partitions": [{"columns": ["entity", "source_system"], "num_buckets": 3}]
  },
  "num_replicas": 1
}'
[ $? -eq 0 ] && echo "   ✓ process_control Table created" || echo "   ✗ Failed"

# ============================================================================
# Table 3: job_dependency
# ============================================================================
echo ""
echo "3. Creating job_dependency Table..."
delete_table_if_exists "job_dependency"

docker exec kudu-master kudu table create $MASTER '{
  "table_name": "job_dependency",
  "schema": {
    "columns": [
      {"column_name": "dependent_job_name", "column_type": "STRING", "is_nullable": false},
      {"column_name": "dependent_job_id", "column_type": "STRING", "is_nullable": false},
      {"column_name": "parent_job_name", "column_type": "STRING", "is_nullable": true},
      {"column_name": "parent_job_id", "column_type": "STRING", "is_nullable": true}
    ],
    "key_column_names": ["dependent_job_name", "dependent_job_id"]
  },
  "partition": {
    "hash_partitions": [{"columns": ["dependent_job_id"], "num_buckets": 3}]
  },
  "num_replicas": 1
}'
[ $? -eq 0 ] && echo "   ✓ job_dependency Table created" || echo "   ✗ Failed"

# ============================================================================
# Table 4: sys_params
# ============================================================================
echo ""
echo "4. Creating sys_params Table..."
delete_table_if_exists "sys_params"

docker exec kudu-master kudu table create $MASTER '{
  "table_name": "sys_params",
  "schema": {
    "columns": [
      {"column_name": "param_key", "column_type": "STRING", "is_nullable": false},
      {"column_name": "environment", "column_type": "STRING", "is_nullable": false},
      {"column_name": "key_name", "column_type": "STRING", "is_nullable": false},
      {"column_name": "param_value", "column_type": "STRING", "is_nullable": true},
      {"column_name": "updated_at", "column_type": "UNIXTIME_MICROS", "is_nullable": true}
    ],
    "key_column_names": ["param_key", "environment", "key_name"]
  },
  "partition": {
    "hash_partitions": [{"columns": ["param_key", "environment"], "num_buckets": 3}]
  },
  "num_replicas": 1
}'
[ $? -eq 0 ] && echo "   ✓ sys_params Table created" || echo "   ✗ Failed"

# ============================================================================
# Table 5: execution_log
# ============================================================================
echo ""
echo "5. Creating execution_log Table..."
delete_table_if_exists "execution_log"

docker exec kudu-master kudu table create $MASTER '{
  "table_name": "execution_log",
  "schema": {
    "columns": [
      {"column_name": "job_id", "column_type": "STRING", "is_nullable": false},
      {"column_name": "id", "column_type": "STRING", "is_nullable": true},
      {"column_name": "run_id", "column_type": "STRING", "is_nullable": true},
      {"column_name": "job_name", "column_type": "STRING", "is_nullable": true},
      {"column_name": "status", "column_type": "STRING", "is_nullable": true},
      {"column_name": "start_time", "column_type": "UNIXTIME_MICROS", "is_nullable": true},
      {"column_name": "end_time", "column_type": "UNIXTIME_MICROS", "is_nullable": true},
      {"column_name": "spark_app_id", "column_type": "STRING", "is_nullable": true}
    ],
    "key_column_names": ["job_id"]
  },
  "partition": {
    "hash_partitions": [{"columns": ["job_id"], "num_buckets": 3}]
  },
  "num_replicas": 1
}'
[ $? -eq 0 ] && echo "   ✓ execution_log Table created" || echo "   ✗ Failed"

# ============================================================================
# Verify All Tables
# ============================================================================
echo ""
echo "======================================================================"
echo "Verifying Created Tables"
echo "======================================================================"
echo ""
docker exec kudu-master kudu table list $MASTER

echo ""
echo "======================================================================"
echo "All Tables Created Successfully!"
echo "======================================================================"
echo ""
echo "Available Tables:"
echo "  1. job_master       - Job parameter configuration"
echo "  2. process_control  - Process scheduling control"
echo "  3. job_dependency   - Job dependency graph"
echo "  4. sys_params       - System configuration"
echo "  5. execution_log    - Job execution history"
echo ""
echo "Next step: Populate tables with data"
echo "  python tools/init_params_kudu.py"
echo "======================================================================"
