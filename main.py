import logging
import sys

from config import config
from utils import get_spark_session
from pipeline import (
    load_transactions,
    load_transaction_types,
    enrich_transactions,
    validate_transactions,
    write_output,
)
from approaches import crossjoin_approach

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def main() -> None:
    """Pipeline entrypoint. Loads data, validates, computes features, writes output."""
    logger.info("Starting pipeline")

    spark = get_spark_session(
        app_name=config.pipeline.app_name,
        log_level=config.pipeline.log_level,
    )

    try:
        transactions_df = load_transactions(spark)
        transaction_types_df = load_transaction_types(spark)

        validate_transactions(transactions_df, transaction_types_df)

        enriched_df = enrich_transactions(transactions_df, transaction_types_df)
        features_df = crossjoin_approach.run(enriched_df, spark)

        features_df.show()
        write_output(features_df)

        logger.info("Pipeline completed successfully")

    except Exception as e:
        logger.exception("Pipeline failed: %s", e)
        sys.exit(1)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
