import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import *  # noqa: F403, F405

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

POSTGRES_URL = "jdbc:postgresql://sb-postgres:5432/safebank"
POSTGRES_PROPERTIES = {
    "user": "safebank",
    "password": "safebank",
    "driver": "org.postgresql.Driver",
}


def save_to_gold(df: DataFrame, table_name: str, partition_cols: list = []):
    """
    Writes data to Gold layer:
    - MinIO (backup):
        - format: delta
        - mode: overwrite
    - Postgres
    """
    target_path = f"s3a://gold/{table_name}"
    logger.info(f"Writing to MinIO: {target_path}...")

    df_final = df.withColumn("processed_at", current_timestamp())

    writer = (
        df_final.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
    )

    if partition_cols:
        logger.info(f"Partitioning by: {partition_cols}")
        writer = writer.partitionBy(*partition_cols)

    writer.save(target_path)

    logger.info(f"Successfully saved {table_name} to MinIO!")

    pg_table_name = f"gold_{table_name}"
    logger.info(f"Syncing to Postgres Table: {pg_table_name}...")

    try:
        df_final.write.jdbc(
            url=POSTGRES_URL,
            table=pg_table_name,
            mode="overwrite",
            properties=POSTGRES_PROPERTIES,
        )
        logger.info(f"Success! Data available in Postgres table: {pg_table_name}")
    except Exception as e:
        logger.error(f"Failed to sync to Postgres: {e}")
