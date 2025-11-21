"""Simple data loader placeholder using PySpark.
Replace with actual implementation in the notebook or scripts.
"""

def load_csv(path, spark=None):
    """Load CSV and return Spark DataFrame (placeholder)."""
    if spark is None:
        raise RuntimeError("SparkSession required")
    return spark.read.option("header","true").option("inferSchema","true").csv(path)
