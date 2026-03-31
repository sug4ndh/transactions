import logging
from typing import List

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from config import config

logger = logging.getLogger(__name__)

c = config.schema_
f = config.features


def compute_feature_1() -> Column:
    """Count of credit wire transactions within the window."""
    return F.sum(
        F.when(
            (F.col(c.transaction_type) == f.wire_type) &
            (F.col(c.credit_debit_code) == f.credit_code),
            1
        ).otherwise(0)
    ).alias(c.feature_1)


def compute_feature_2() -> Column:
    """Count of debit wire transactions within the window."""
    return F.sum(
        F.when(
            (F.col(c.transaction_type) == f.wire_type) &
            (F.col(c.credit_debit_code) == f.debit_code),
            1
        ).otherwise(0)
    ).alias(c.feature_2)


def compute_feature_3() -> Column:
    """Total value of card transactions (credit + debit) within the window."""
    return F.sum(
        F.when(
            F.col(c.transaction_type) == f.card_type,
            F.col(c.transaction_value)
        ).otherwise(0)
    ).alias(c.feature_3)


def compute_all_features(windowed_df: DataFrame, ref_dates: List[str]) -> DataFrame:
    """Aggregate all three features grouped by ref_date and customer_id.
    Expects a DataFrame that already has a ref_date column and has been
    filtered to the relevant 7-day window per ref_date.
    """
    logger.info("Computing features for ref_dates: %s", ref_dates)
    return (
        windowed_df
        .groupBy(c.ref_date, c.customer_id)
        .agg(
            compute_feature_1(),
            compute_feature_2(),
            compute_feature_3(),
        )
        .filter(F.col(c.ref_date).isin([F.lit(d) for d in ref_dates]))
        .select(c.ref_date, c.customer_id, c.feature_1, c.feature_2, c.feature_3)
        .orderBy(c.ref_date, c.customer_id)
    )
