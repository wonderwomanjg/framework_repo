import os
import sys
from pyspark.sql import SparkSession

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__))) #-->C:\MyFiles\AIA\vsA
#alternate : sys.path.insert(0, r"C:\MyFiles\AIA\vsA")

# Set local warehouse and Derby home to avoid embedded metastore/native issues on Windows
warehouse_dir = r"C:\tmp\spark-warehouse"
derby_home = r"C:\tmp\derby-home"
os.makedirs(warehouse_dir, exist_ok=True)
os.makedirs(derby_home, exist_ok=True)

from framework.logging import get_logger ##--> import logging.py 

def main():
    log = get_logger("create_table")
    csv_path = r"C:\MyFiles\AIA\vsA\cust.CSV"   # use a sample CSV from this repo
    db_table = "si_sg.tb_si_CRM_customer_contract"

    #spark = (SparkSession.builder
    #        .appName("create_table")
    #       .master("local[*]")
    #      .enableHiveSupport()
        #     .getOrCreate())



    spark = (
        SparkSession.builder
        .appName("orchestrator_param_fetch")
        .master("local[*]")
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .config("spark.hadoop.derby.system.home", derby_home)
        .enableHiveSupport()
        .getOrCreate()
    )


    spark.sql("CREATE DATABASE IF NOT EXISTS si_sg")
    df = spark.read.option("header", True).csv(csv_path)
    #df.write.mode("overwrite").saveAsTable(db_table)

    print("Created databse: si_sg")
    spark.sql("show databases").show()
    #df.show(5)

    import os
    out_path = r"C:\MyFiles\AIA\vsA\data\customer_contract_parquet"
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    #spark = SparkSession.builder.master("local[*]").getOrCreate()
    #df = spark.read.option("header", True).csv(csv_path)
    #df.write.mode("overwrite").parquet(out_path)
    #print("Wrote orc to:", out_path)



    df.createOrReplaceTempView("customer_contract")

    # Query using Spark SQL
    spark.sql("SELECT * FROM customer_contract LIMIT 5").show()


    import pandas as pd
    import json
    pdf = pd.read_csv(r"C:\MyFiles\AIA\vsA\cust.CSV")
    pdf_data=pdf.to_orc(r"C:\MyFiles\AIA\vsA\data\customer_contract.orc", index=False)

    df_orc = spark.read.orc(r"C:\MyFiles\AIA\vsA\data\customer_contract.orc")
    df_orc.show(5)

    orc_path = r"C:\MyFiles\AIA\vsA\data\customer_contract.orc"
    #save this orc path in params and read it in landing job
    job_params = {
        "orc_path": r"C:\MyFiles\AIA\vsA\data\customer_contract.orc"
    }
    os.environ["JOB_PARAMS"] = json.dumps(job_params)

    print("Created table: si_sg.tb_si_CRM_customer_contract")
    spark.stop()

    return orc_path

if __name__ == "__main__":
    main()


## Imp JG Note : register spark table is not working in this script that's why created
#  a new pandas function to create orc file and read it using spark.
#but in real scenario we can create table using spark sql commands.
