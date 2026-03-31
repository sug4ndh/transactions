import logging
import os

from pyspark.sql import DataFrame

from config import config

logger = logging.getLogger(__name__)


def write_output(df: DataFrame) -> None:
    """Write the features DataFrame as a single CSV to the configured output path.

    coalesce(1) is appropriate here given the small output size.
    For large outputs, remove it and let Spark write part files.
    """
    output_path = os.path.join(config.paths.output_dir, config.paths.output_filename)
    logger.info("Writing output to: %s", output_path)
    (
        df.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(output_path)
    )
