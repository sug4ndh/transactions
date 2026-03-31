import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DoubleType

from config import config

logger = logging.getLogger(__name__)

c = config.schema_


def load_transactions(spark: SparkSession) -> DataFrame:
    """Load Transactions.csv and cast columns to their correct types."""
    logger.info("Loading transactions from: %s", config.paths.transactions)
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(config.paths.transactions)
        .withColumn(c.transaction_date, F.col(c.transaction_date).cast(DateType()))
        .withColumn(c.transaction_value, F.col(c.transaction_value).cast(DoubleType()))
    )


def load_transaction_types(spark: SparkSession) -> DataFrame:
    """Load TransactionType.csv and normalise transaction_type to lowercase."""
    logger.info("Loading transaction types from: %s", config.paths.transaction_types)
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(config.paths.transaction_types)
        .withColumn(c.transaction_type, F.lower(F.col(c.transaction_type)))
    )


def enrich_transactions(transactions_df: DataFrame, transaction_types_df: DataFrame) -> DataFrame:
    """Join transactions with transaction types.

    TransactionType is a small static lookup table so we broadcast it explicitly
    to avoid shuffling the large transactions dataset across the cluster.
    """
    logger.info("Joining transactions with transaction types")
    return transactions_df.join(
        F.broadcast(transaction_types_df),
        on=c.transaction_type_code,
        how="inner",
    )
