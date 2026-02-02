r'''
cd C:\MyFiles\GE\Orch\jobs
C:\spark\bin\spark-submit.cmd `
  --master local[2] `
  --driver-memory 1g `
  --executor-memory 1g `
  --conf "spark.pyspark.python=C:\MyFiles\GE\Orch\.venv311\Scripts\python.exe" `
  --conf "spark.pyspark.driver.python=C:\MyFiles\GE\Orch\.venv311\Scripts\python.exe" `
  test_job.py

  for cloudera
  spark-submit --master yarn --deploy-mode cluster my_job.py
'''

from pyspark.sql import SparkSession
import os

def main():
    # Start Spark
    spark = (SparkSession.builder.appName("spark_submit_test_job").getOrCreate())
    data = [("A", 100), ("B", 200), ("C", 300)]
    df = spark.createDataFrame(data, ["category", "value"])
    df.show()
    output_path = r"C:/MyFiles/GE/Orch/out/"
    df.write.mode("overwrite").orc(output_path)
    print("===== Job Finished Successfully =====")
    spark.stop()

if __name__ == "__main__":
    main()