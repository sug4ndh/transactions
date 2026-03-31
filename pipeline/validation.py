import logging
from typing import Set

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config import config

logger = logging.getLogger(__name__)

c = config.schema_
f = config.features

REQUIRED_COLUMNS = {
    c.customer_id,
    c.transaction_id,
    c.transaction_date,
    c.credit_debit_code,
    c.transaction_type_code,
    c.transaction_value,
}


def check_required_columns(transactions_df: DataFrame) -> None:
    """Raise ValueError if any expected column is missing from the DataFrame."""
    missing = REQUIRED_COLUMNS - set(transactions_df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def check_for_new_columns(transactions_df: DataFrame) -> None:
    """Log a warning if the source data contains columns not in the known schema."""
    new_cols = set(transactions_df.columns) - REQUIRED_COLUMNS
    if new_cols:
        logger.warning("New columns detected in source data: %s", new_cols)


def check_no_nulls(transactions_df: DataFrame) -> None:
    """Raise ValueError if any critical column contains null values."""
    critical_columns = [c.customer_id, c.transaction_id, c.transaction_date]
    for col in critical_columns:
        null_count = transactions_df.filter(F.col(col).isNull()).count()
        if null_count > 0:
            raise ValueError(f"Column '{col}' contains {null_count} null values")


def check_credit_debit_codes(transactions_df: DataFrame) -> None:
    """Raise ValueError if credit_debit_code contains values other than C or D."""
    valid_codes = [f.credit_code, f.debit_code]
    invalid = (
        transactions_df
        .filter(~F.col(c.credit_debit_code).isin(valid_codes))
        .count()
    )
    if invalid > 0:
        raise ValueError(
            f"Found {invalid} rows with invalid credit_debit_code. Expected one of {valid_codes}"
        )


def check_transaction_values(transactions_df: DataFrame) -> None:
    """Raise ValueError if any transaction_value is negative."""
    negative = (
        transactions_df
        .filter(F.col(c.transaction_value) < 0)
        .count()
    )
    if negative > 0:
        raise ValueError(f"Found {negative} rows with negative transaction_value")


def check_referential_integrity(transactions_df: DataFrame, transaction_types_df: DataFrame) -> None:
    """Raise ValueError if any transaction_type_code has no match in TransactionType."""
    known_codes: Set[str] = {
        row[c.transaction_type_code]
        for row in transaction_types_df.select(c.transaction_type_code).collect()
    }
    unknown = (
        transactions_df
        .select(c.transaction_type_code)
        .distinct()
        .filter(~F.col(c.transaction_type_code).isin(list(known_codes)))
        .collect()
    )
    if unknown:
        unknown_codes = [r[c.transaction_type_code] for r in unknown]
        raise ValueError(f"Unknown transaction_type_codes found: {unknown_codes}")


def validate_transactions(transactions_df: DataFrame, transaction_types_df: DataFrame) -> None:
    """Run all validation checks against the raw transactions DataFrame.

    Checks run in order — required columns first so subsequent checks
    can safely reference those columns without risk of AnalysisException.
    """
    logger.info("Running validation checks")
    check_required_columns(transactions_df)
    check_for_new_columns(transactions_df)
    check_no_nulls(transactions_df)
    check_credit_debit_codes(transactions_df)
    check_transaction_values(transactions_df)
    check_referential_integrity(transactions_df, transaction_types_df)
    logger.info("All validation checks passed")