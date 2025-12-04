import sys
import os
import logging

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../../"))
)

from scripts.spark.utils.spark_connector import create_spark_session
from scripts.spark.jobs.gold import gold_utils
from pyspark.sql.functions import *  # noqa: F403, F405

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


def run_financial_mart():
    logger.info("Starting daily financial performance mart...")
    spark = create_spark_session("safebank_gold_financial")

    # loading silver data
    logger.info("Loading Silver data...")

    df_transfer = (
        spark.read.format("delta")
        .load("s3a://silver/transfer")
        .filter(col("status") == "SUCCESS")
        .select(
            "from_account_id",
            "to_account_id",
            "amount",
            "currency_code",
            "txn_time",
            "channel_id",
        )
    )

    df_account = (
        spark.read.format("delta")
        .load("s3a://silver/account")
        .select(col("id").alias("account_id"), "branch_id")
    )

    df_branch = (
        spark.read.format("delta")
        .load("s3a://silver/branch")
        .select(col("id").alias("branch_id"), col("city").alias("branch_city"))
    )

    df_channel = (
        spark.read.format("delta")
        .load("s3a://silver/channel")
        .select(col("id").alias("channel_id"), "channel_code")
    )

    df_rate = (
        spark.read.format("delta")
        .load("s3a://silver/exchange_rate")
        .select("from_currency", "rate", "effective_date")
    )

    # join dimensions
    logger.info("Enriching transaction data...")
    df_transfer_date = df_transfer.withColumn("txn_date", to_date(col("txn_time")))

    df_enriched = (
        df_transfer_date.join(
            df_account,
            df_transfer_date.from_account_id == df_account.account_id,
            "left",
        )
        .join(df_branch, df_account.branch_id == df_branch.branch_id, "left")
        .join(df_channel, df_transfer_date.channel_id == df_channel.channel_id, "left")
    )

    # currency convention
    logger.info("Applying currency conversion...")
    df_with_rate = df_enriched.join(
        df_rate,
        (df_enriched.currency_code == df_rate.from_currency)
        & (df_enriched.txn_date == df_rate.effective_date),
        "left",
    )

    df_calculated = df_with_rate.withColumn(
        "applied_rate",
        when(col("currency_code") == "VND", lit(1.0)).otherwise(
            coalesce(col("rate"), lit(1.0))
        ),
    ).withColumn("amount_vnd", col("amount") * col("applied_rate"))

    # aggregation and reporting
    logger.info("Aggregating daily performance...")

    df_report = df_calculated.groupBy(
        col("txn_date").alias("report_date"),
        "branch_city",
        "channel_code",
        "currency_code",
    ).agg(
        count("amount").alias("total_transactions"),
        sum("amount").alias("total_amount_orginal"),
        sum("amount_vnd").alias("total_amount_vnd"),
    )

    gold_utils.save_to_gold(
        df_report, "daily_financial_performance", partition_cols=["report_date"]
    )

    spark.stop()


if __name__ == "__main__":
    run_financial_mart()
