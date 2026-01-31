
from pyspark.sql import SparkSession

def apply_profile(spark: SparkSession, profile: str = "medium") -> None:
    cfg = {
        "small":  {"spark.sql.shuffle.partitions": "100",  "spark.sql.adaptive.enabled": "true"},
        "medium": {"spark.sql.shuffle.partitions": "400",  "spark.sql.adaptive.enabled": "true"},
        "large":  {"spark.sql.shuffle.partitions": "1200", "spark.sql.adaptive.enabled": "true"},
    }
    for k, v in cfg.get(profile, {}).items():
        spark.conf.set(k, v)
