
from pyspark.sql import SparkSession, DataFrame

# ----- Readers -----

def read_csv(spark: SparkSession, path: str, schema=None, opts=None) -> DataFrame:
    _opts = {"header": "true", "mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_bad_record"}
    if opts: _opts.update(opts)
    reader = spark.read.format("csv").options(**_opts)
    if schema: reader = reader.schema(schema)
    return reader.load(path)

def read_json(spark: SparkSession, path: str, schema=None, opts=None) -> DataFrame:
    _opts = {"primitivesAsString": "false", "mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_bad_record"}
    if opts: _opts.update(opts)
    reader = spark.read.format("json").options(**_opts)
    if schema: reader = reader.schema(schema)
    return reader.load(path)

def read_orc(spark: SparkSession, path: str, schema=None, opts=None) -> DataFrame:
    """
    ORC reader. Useful options:
      mergeSchema='true', recursiveFileLookup='true', compression='snappy' (on write)
    """
    reader = spark.read.format("orc")
    if opts: reader = reader.options(**opts)
    if schema: reader = reader.schema(schema)
    return reader.load(path)

def read_parquet(spark: SparkSession, path: str, schema=None, opts=None) -> DataFrame:
    """
    ORC reader. Useful options:
      mergeSchema='true', recursiveFileLookup='true', compression='snappy' (on write)
    """
    reader = spark.read.format("orc")
    if opts: reader = reader.options(**opts)
    if schema: reader = reader.schema(schema)
    return reader.load(path)


# ----- Writers -----

def write_df(df: DataFrame, path: str, mode: str = "append", fmt: str = "parquet", partitionBy=None, opts=None) -> None:
    writer = df.write.mode(mode).format(fmt)
    if partitionBy: writer = writer.partitionBy(*partitionBy)
    if opts: writer = writer.options(**opts)
    writer.save(path)

def write_orc(df: DataFrame, path: str, mode: str = "append", partitionBy=None, opts=None) -> None:
    """
    ORC writer. Common opts: {'compression':'snappy'}.
    """
    write_df(df, path, mode=mode, fmt="orc", partitionBy=partitionBy, opts=opts)

# ----- Atomic publish -----

def atomic_publish(df: DataFrame, final_path: str, tmp_path: str, mode: str = "overwrite", fmt: str = "parquet",
                   partitionBy=None, opts=None, spark: SparkSession=None) -> None:
    """
    Write to a temp path, then rename to final (safer publish).
    """
    write_df(df, tmp_path, mode="overwrite", fmt=fmt, partitionBy=partitionBy, opts=opts)

    sc = (spark or df.sparkSession).sparkContext
    hconf = sc._jsc.hadoopConfiguration()
    URI = sc._jvm.java.net.URI
    Path = sc._jvm.org.apache.hadoop.fs.Path
    FS = sc._jvm.org.apache.hadoop.fs.FileSystem

    src = Path(tmp_path)
    dst = Path(final_path)
    fs = FS.get(URI(final_path), hconf)

    if fs.exists(dst) and mode == "overwrite":
        fs.delete(dst, True)
    if not fs.rename(src, dst):
        raise RuntimeError(f"Atomic publish failed: cannot rename {tmp_path} -> {final_path}")
    success = Path(final_path.rstrip("/") + "/_SUCCESS")
    fs.create(success).close()
