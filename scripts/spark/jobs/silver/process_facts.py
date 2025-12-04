import sys
import os
import logging
from concurrent.futures import ThreadPoolExecutor

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../../"))
)

from scripts.spark.utils.spark_connector import create_spark_session
from scripts.spark.jobs.silver import silver_utils
from scripts.spark.jobs.silver import cleaning_utils

# from pyspark.sql.functions import *

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


def process_transfer(df, batch_id):
    logger.info(f"Processing batch {batch_id} for Transfer")

    df_clean = (
        df.withColumn("amount", cleaning_utils.clean_money("amount"))
        .withColumn("txn_time", cleaning_utils.clean_timestamp("txn_time"))
        .withColumn("create_time", cleaning_utils.clean_timestamp("create_time"))
    )

    silver_utils.upsert_scd1(df_clean, batch_id, "s3a://silver/transfer", ["id"])


def process_loan_payment(df, batch_id):
    logger.info(f"Processing batch {batch_id} for Loan Payment")

    df_clean = df.withColumn("amount", cleaning_utils.clean_money("amount")).withColumn(
        "payment_date", cleaning_utils.clean_timestamp("payment_date")
    )

    silver_utils.upsert_scd1(df_clean, batch_id, "s3a://silver/loan_payment", ["id"])


def process_exchange_rate(df, batch_id):
    logger.info(f"Processing batch {batch_id} for Exchange Rate")

    df_clean = df.withColumn("rate", cleaning_utils.clean_money("rate")).withColumn(
        "effective_date", cleaning_utils.convert_validate_birthday("effective_date")
    )

    silver_utils.upsert_scd1(df_clean, batch_id, "s3a://silver/exchange_rate", ["id"])


def process_sign_in(df, batch_id):
    logger.info(f"Processing batch {batch_id} for Sign In")

    df_clean = df.withColumn(
        "sign_in_time", cleaning_utils.clean_timestamp("sign_in_time")
    )

    silver_utils.upsert_scd1(df_clean, batch_id, "s3a://silver/sign_in", ["id"])


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
        ("transfer", process_transfer),
        ("loan_payment", process_loan_payment),
        ("exchange_rate", process_exchange_rate),
        ("sign_in", process_sign_in),
    ]

    MAX_WORKERS = 2
    logger.info(f"Starting Silver Processing with pool size: {MAX_WORKERS}")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(run_silver_job, spark, cfg) for cfg in jobs_config]

        for future in futures:
            future.result()

    logger.info("All Silver Fact tables processed successfully!")
    spark.stop()


if __name__ == "__main__":
    main()
