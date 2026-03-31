import datetime
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, DateType
)
from pyspark.sql import functions as F

from pipeline.validation import (
    validate_transactions,
    check_referential_integrity,
)


TRANSACTIONS_SCHEMA = StructType([
    StructField("customer_id", StringType(), False),
    StructField("transaction_id", StringType(), False),
    StructField("transaction_date", DateType(), False),
    StructField("credit_debit_code", StringType(), False),
    StructField("transaction_type_code", StringType(), False),
    StructField("transaction_value", DoubleType(), False),
])

TYPES_SCHEMA = StructType([
    StructField("transaction_type_code", StringType(), False),
    StructField("transaction_type", StringType(), False),
])


def make_valid_transactions(spark: SparkSession):
    return spark.createDataFrame(
        [("ABC", "T001", datetime.date(2025, 6, 30) , "C", "TXN01", 100.0)],
        schema=TRANSACTIONS_SCHEMA,
    ).withColumn("transaction_date", F.to_date(F.col("transaction_date"), "yyyy-MM-dd"))


def make_valid_types(spark: SparkSession):
    return spark.createDataFrame(
        [("TXN01", "wire")],
        schema=TYPES_SCHEMA,
    )


def test_valid_data_passes(spark: SparkSession):
    """Well-formed data passes all validation checks without errors."""
    txn = make_valid_transactions(spark)
    types = make_valid_types(spark)
    validate_transactions(txn, types)


def test_unknown_transaction_type_code_fails(spark: SparkSession):
    """A transaction_type_code not present in TransactionType raises ValueError."""
    txn = make_valid_transactions(spark)
    types = spark.createDataFrame(
        [("TXN99", "wire")],
        schema=TYPES_SCHEMA,
    )

    with pytest.raises(ValueError, match="Unknown transaction_type_codes"):
        check_referential_integrity(txn, types)
