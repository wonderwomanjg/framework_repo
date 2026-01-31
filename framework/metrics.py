
import time
from contextlib import contextmanager
from pyspark.sql import DataFrame

@contextmanager
def timed(logger, activity: str):
    t0 = time.time()
    try:
        yield
    finally:
        logger.info(f"{activity} completed in {round(time.time() - t0, 2)} sec")

def rows_processed(df: DataFrame, logger, label: str = "rows_processed") -> int:
    cnt = df.count()
    logger.info(f"{label}: {cnt}")
    return cnt
