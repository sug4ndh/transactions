import logging
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def get_spark_session(app_name: str, log_level: str = "ERROR") -> SparkSession:
    """Build and return a configured SparkSession.

    Uses local[*] for development. On EMR Serverless the master is
    provided by the cluster and this setting is ignored automatically.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(log_level)
    logger.info("Spark session started (version %s)", spark.version)
    return spark
