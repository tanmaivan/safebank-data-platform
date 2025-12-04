import sys
import os
import logging
from concurrent.futures import ThreadPoolExecutor

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../../"))
sys.path.append(project_root)

from pyspark.sql.functions import col, from_json  # noqa: E402
from spark.utils.spark_connector import create_spark_session  # noqa: E402
from spark.utils import schemas  # noqa: E402

# logging configuration
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


def ingest_table(spark, config):
    """
    Reads from Kafka, parses JSON and writes to Delta Lake (Bronze).
    """
    topic_name, table_name, schema = config

    logging.info(
        f"[START] Starting stream for Table: {table_name} | Topic: {topic_name}"
    )

    try:
        # read stream from kafka
        raw_stream = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", "sb-broker:29092")
            .option("subscribe", topic_name)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .option("maxOffsetsPerTrigger", 5000)
            .load()
        )

        # parse json using debezium envelop
        debezium_schema = schemas.get_debezium_schema(schema)
        parsed_stream = raw_stream.select(
            from_json(col("value").cast("string"), debezium_schema).alias("data")
        )

        # extract and flatten
        bronze_stream = parsed_stream.select(
            col("data.payload.after.*"),
            col("data.payload.op").alias("cdc_operation"),
            col("data.payload.ts_ms").alias("cdc_timestamp"),
        )

        # write stream to delta lake (minio)
        checkpoint_path = f"s3a://checkpoints/bronze/{table_name}"
        output_path = f"s3a://bronze/{table_name}"

        query = (
            bronze_stream.writeStream.format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint_path)
            .option("path", output_path)
            .trigger(availableNow=True)
            .start()
        )

        query.awaitTermination()
        logging.info(f"[DONE] Finished table: {table_name}")

        return True

    except Exception as e:
        logging.error(f"[ERROR] Table {table_name}: {e}")
        return False


def main():
    spark = create_spark_session("safebank_bronze_ingestion")

    # List of tables
    tables_to_ingest = [
        # Dimensions
        ("sb-server.public.branch", "branch", schemas.branch_schema),
        ("sb-server.public.currency", "currency", schemas.currency_schema),
        ("sb-server.public.channel", "channel", schemas.channel_schema),
        ("sb-server.public.merchant", "merchant", schemas.merchant_schema),
        ("sb-server.public.device", "device", schemas.device_schema),
        # sb-server Entities
        ("sb-server.public.person", "person", schemas.person_schema),
        ("sb-server.public.account", "account", schemas.account_schema),
        ("sb-server.public.loan_account", "loan_account", schemas.loan_account_schema),
        # Facts
        ("sb-server.public.transfer", "transfer", schemas.transfer_schema),
        ("sb-server.public.sign_in", "sign_in", schemas.sign_in_schema),
        ("sb-server.public.loan_payment", "loan_payment", schemas.loan_payment_schema),
        (
            "sb-server.public.exchange_rate",
            "exchange_rate",
            schemas.exchange_rate_schema,
        ),
    ]

    MAX_WORKERS = 3
    logging.info(f"Starting ingestion with pool size: {MAX_WORKERS}")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        list(executor.map(lambda cfg: ingest_table(spark, cfg), tables_to_ingest))

    logging.info("All tables processed. Job finished!")


if __name__ == "__main__":
    main()
