"""
Spark session factory for local mode PySpark.

This module provides a centralized way to create and configure Spark sessions.
Using a factory function ensures consistent configuration across the project
and makes it easy to adjust Spark settings for performance tuning.
"""

from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "MovieLensAnalysis", memory: str = "4g") -> SparkSession:
    """
    Create a Spark session configured for local mode.

    Local mode is appropriate for this laptop-scale dataset (~1MB).
    For larger datasets, you would connect to a cluster manager like YARN or Kubernetes.

    Args:
        app_name: Name of the Spark application (appears in Spark UI)
        memory: Executor memory allocation. 4g is sufficient for MovieLens small dataset.

    Returns:
        Configured SparkSession instance

    Performance considerations:
        - local[*] uses all available cores on the machine
        - Setting memory too high can cause out-of-memory errors on laptops
        - Setting memory too low can cause excessive disk spilling
    """
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.executor.memory", memory)
        .config("spark.driver.memory", memory)
        # Reduce shuffle partitions for local mode - default 200 is overkill for small data
        .config("spark.sql.shuffle.partitions", "8")
        # Enable adaptive query execution (Spark 3.x feature for automatic optimization)
        .config("spark.sql.adaptive.enabled", "true")
    )

    spark = builder.getOrCreate()

    # Set log level to WARN to reduce console noise during development
    spark.sparkContext.setLogLevel("WARN")

    return spark
