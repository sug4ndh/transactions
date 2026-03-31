import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from config import config
from pipeline.features import compute_all_features

logger = logging.getLogger(__name__)

c = config.schema_
p = config.pipeline
f = config.features


def build_windowed_df(enriched_df: DataFrame, spark: SparkSession) -> DataFrame:
    """Cross join enriched transactions against the ref_dates table and filter
    to the 7-day window for each ref_date.
    """
    ref_dates_df = spark.createDataFrame(
        [(d,) for d in p.ref_dates],
        [c.ref_date]
    ).withColumn(c.ref_date, F.to_date(F.col(c.ref_date), "yyyy-MM-dd"))

    return (
        enriched_df
        .crossJoin(ref_dates_df)
        .filter(
            (F.col(c.transaction_date) >= F.date_sub(F.col(c.ref_date), f.window_days - 1)) &
            (F.col(c.transaction_date) <= F.col(c.ref_date))
        )
    )


def run(enriched_df: DataFrame, spark: SparkSession) -> DataFrame:
    """Execute the cross join approach and return the final features DataFrame."""
    logger.info("Running cross join approach")
    windowed_df = build_windowed_df(enriched_df, spark)
    return compute_all_features(windowed_df, p.ref_dates)
