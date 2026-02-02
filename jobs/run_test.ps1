# Run test job with proper Python configuration
$pythonExe = "C:\MyFiles\GE\Orch\.venv311\Scripts\python.exe"

C:\spark\bin\spark-submit.cmd `
  --master local[2] `
  --driver-memory 1g `
  --executor-memory 1g `
  --conf "spark.pyspark.python=$pythonExe" `
  --conf "spark.pyspark.driver.python=$pythonExe" `
  test_job.py
