import sys
import os
import logging
from concurrent.futures import ThreadPoolExecutor
from functools import partial

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../../"))
)

from scripts.spark.utils.spark_connector import create_spark_session
from scripts.spark.jobs.silver import silver_utils
from scripts.spark.jobs.silver import cleaning_utils

# from pyspark.sql.functions import *
from pyspark.sql.functions import col

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


def process_person(df, batch_id):
    """
    Clean -> Deduplicate -> Merge SCD2 for Person table
    """
    logger.info(f"Processing batch {batch_id} for Person...")

    df_clean = (
        df.withColumn("name", cleaning_utils.clean_text("name"))
        .withColumn("gender", cleaning_utils.clean_gender("gender"))
        .withColumn("birthday", cleaning_utils.convert_validate_birthday("birthday"))
        .withColumn("country", cleaning_utils.handle_null_string("country"))
        .withColumn("city", cleaning_utils.handle_null_string("city"))
        .withColumn("create_time", cleaning_utils.clean_timestamp("create_time"))
        .withColumn("update_time", cleaning_utils.clean_timestamp("update_time"))
    )

    silver_utils.upsert_scd2(
        micro_batch_df=df_clean,
        batch_id=batch_id,
        target_path="s3a://silver/person",
        unique_keys=["id"],
    )


def process_account(df, batch_id):
    """
    Clean -> Deduplicate -> Merge SCD2 for Account table
    """
    logger.info(f"Processing batch {batch_id} for Account...")

    df_clean = (
        df.withColumn(
            "account_type", cleaning_utils.normalize_account_types("account_type")
        )
        .withColumn("phone_number", cleaning_utils.clean_phone("phone_number"))
        .withColumn(
            "email", cleaning_utils.handle_null_string("email", "no-email@safebank.com")
        )
        .withColumn("balance", cleaning_utils.clean_money("balance"))
        .withColumn("create_time", cleaning_utils.clean_timestamp("create_time"))
        .withColumn("update_time", cleaning_utils.clean_timestamp("update_time"))
    )

    silver_utils.upsert_scd2(
        micro_batch_df=df_clean,
        batch_id=batch_id,
        target_path="s3a://silver/account",
        unique_keys=["id"],
    )


def process_loan_account(df, batch_id):
    logger.info(f"Processing batch {batch_id} for Loan Account...")

    df_clean = (
        df.withColumn("amount", cleaning_utils.clean_money("amount"))
        .withColumn(
            "remaining_balance", cleaning_utils.clean_money("remaining_balance")
        )
        .withColumn(
            "start_date", cleaning_utils.convert_validate_birthday("start_date")
        )
        .withColumn("end_date", cleaning_utils.convert_validate_birthday("end_date"))
    )

    silver_utils.upsert_scd2(
        micro_batch_df=df_clean,
        batch_id=batch_id,
        target_path="s3a://silver/loan_account",
        unique_keys=["id"],
    )


def process_device(df, batch_id):
    logger.info(f"Processing batch {batch_id} for Device")

    df_clean = (
        df.withColumn("device_model", cleaning_utils.clean_text("device_model"))
        .withColumn("device_fingerprint", col("device_fingerprint"))
        .withColumn("os_version", cleaning_utils.clean_device_version("os_version"))
    )

    silver_utils.upsert_scd1(
        df_clean, batch_id, "s3a://silver/device", ["device_fingerprint"]
    )


def process_reference(df, batch_id, table_name, keys=["id"]):
    logger.info(f"Processing batch {batch_id} for {table_name}...")

    df_clean = df
    # table Branch
    if "open_date" in df.columns:
        df_clean = df_clean.withColumn(
            "open_date", cleaning_utils.convert_validate_birthday("open_date")
        )

    # trim string columns
    for field in df.schema.fields:
        if str(field.dataType) == "StringType":
            df_clean = df_clean.withColumn(
                field.name, cleaning_utils.clean_text(field.name)
            )

    # scd type 1
    silver_utils.upsert_scd1(
        micro_batch_df=df_clean,
        batch_id=batch_id,
        target_path=f"s3a://silver/{table_name}",
        unique_keys=keys,
    )


def run_silver_job(spark, config):
    """
    Generic function to run a Silver Stream with Trigger AvailableNow
    """
    table_name, logic_func = config
    logger.info(f"[START] Processing silver table: {table_name}")

    try:
        bronze_path = f"s3a://bronze/{table_name}"
        df_stream = spark.readStream.format("delta").load(bronze_path)

        query = (
            df_stream.writeStream.foreachBatch(logic_func)
            .outputMode("update")
            .option("checkpointLocation", f"s3a://checkpoints/silver/{table_name}")
            .trigger(availableNow=True)
            .start()
        )

        query.awaitTermination()
        logger.info(f"[DONE] Finished silver table: {table_name}")

        return True

    except Exception as e:
        logger.error(f"[ERROR] Failed to process {table_name}: {e}")

        return False


def main():
    spark = create_spark_session("safebank_silver_dims")

    jobs_config = [
        ("person", process_person),
        ("account", process_account),
        ("loan_account", process_loan_account),
        ("device", process_device),
        ("branch", partial(process_reference, table_name="branch", keys=["id"])),
        ("currency", partial(process_reference, table_name="currency", keys=["code"])),
        ("channel", partial(process_reference, table_name="channel", keys=["id"])),
        ("merchant", partial(process_reference, table_name="merchant", keys=["id"])),
    ]

    MAX_WORKERS = 3
    logger.info(f"Starting Silver Processing with pool size: {MAX_WORKERS}")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(run_silver_job, spark, cfg) for cfg in jobs_config]

        for future in futures:
            future.result()
    logger.info("All Silver Dimension tables processed successfully!")
    spark.stop()


if __name__ == "__main__":
    main()
