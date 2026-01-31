
import os, argparse, json, shlex
from pyspark.sql import SparkSession
from framework.jdbc import read_oracle_kv
from framework.logging import get_logger

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--entity", "--e", required=True)
    ap.add_argument("--source_system", "--s", required=True)
    ap.add_argument("--child_job", "--j", required=True)
    ap.add_argument("--module_name", "--m", required=True)
    args = ap.parse_args()

    spark = SparkSession.builder.appName("submit_cmd_builder").getOrCreate()
    log = get_logger("submit_builder")

    where = (f"module_name='{args.module_name}' AND entity='{args.entity}' "
             f"AND source_system='{args.source_system}' AND child_job='{args.child_job}'")
    kv = read_oracle_kv(
        spark,
        url=os.environ["ORACLE_JDBC_URL"],
        user=os.environ["ORACLE_USER"],
        password=os.environ["ORACLE_PASSWORD"],
        table=os.environ.get("ORACLE_PARAM_TABLE", "JOB_PARAM_REGISTRY"),
        where_sql=where
    )
    spark.stop()

    driver_mem     = kv.get("spark.driver.memory", "4g")
    executor_mem   = kv.get("spark.executor.memory", "8g")
    executor_cores = kv.get("spark.executor.cores", "2")
    num_executors  = kv.get("spark.executor.instances", "4")
    extra_confs    = kv.get("spark.extraConfs", "")  # comma-separated "k=v,k=v"

    job_params = json.dumps({
        "entity": args.entity,
        "source_system": args.source_system,
        "table_name": kv["table_name"],
        "source_schema": kv["source_schema"],
        "landing_schema": kv.get("landing_schema", "fdm_landing"),
        "biz_dt": kv["biz_dt"],
        "business_keys": kv["business_keys"],
        "compare_cols": kv["compare_cols"]
    })

    cmd = [
        "spark-submit",
        "--name", f"landing_{args.entity}_{args.source_system}_{args.child_job}",
        "--master", kv.get("spark.master", "yarn"),
        "--deploy-mode", kv.get("spark.deploy.mode", "cluster"),
        "--driver-memory", driver_mem,
        "--executor-memory", executor_mem,
        "--executor-cores", executor_cores,
        "--conf", f"spark.executor.instances={num_executors}",
        "--conf", "spark.sql.catalogImplementation=hive",
        "--conf", "spark.sql.warehouse.dir=/user/hive/warehouse",
        "--conf", f"spark.executorEnv.JOB_PARAMS={job_params}",
        "--conf", f"spark.driver.extraJavaOptions=-DJOB_PARAMS='{job_params}'",
        # Ensure Oracle JDBC jar is supplied via --jars or on classpath when you run this.
        "jobs/landing_fpms_contract_master.py"
    ]
    if extra_confs:
        for kvpair in extra_confs.split(","):
            cmd.insert(-1, "--conf")
            cmd.insert(-1, kvpair.strip())

    print(" ".join(shlex.quote(x) for x in cmd))

if __name__ == "__main__":
    main()
