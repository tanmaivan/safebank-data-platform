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


def run_merchant_mart():
    logger.info("Starting Merchant and Consumption Analysis Mart...")
    spark = create_spark_session("safebank_gold_merchant")

    # load data
    logger.info("Loading Silver Data...")

    df_transfer = (
        spark.read.format("delta")
        .load("s3a://silver/transfer")
        .filter((col("status") == "SUCCESS") & (col("merchant_id").isNotNull()))
        .select("from_account_id", "merchant_id", "amount", "txn_time")
        .withColumn("txn_date", to_date(col("txn_time")))
    )

    df_merchant = (
        spark.read.format("delta")
        .load("s3a://silver/merchant")
        .select(col("id").alias("merch_id"), "name", "category")
    )

    df_account = (
        spark.read.format("delta")
        .load("s3a://silver/account")
        .filter(col("is_current") == True)
        .select(col("id").alias("account_id"), "owner_id")
    )

    df_person = (
        spark.read.format("delta")
        .load("s3a://silver/person")
        .filter(col("is_current") == True)
        .select(col("id").alias("person_id"), "gender", "city", "birthday")
    )

    # feature engineering
    df_person_feat = df_person.withColumn(
        "age", year(current_date()) - year(col("birthday"))
    ).withColumn(
        "age_group",
        when(col("age") < 25, "Gen Z (<25)")
        .when((col("age") >= 25) & (col("age") < 40), "Millennials (25-40)")
        .when((col("age") >= 40) & (col("age") < 60), "Gen X (40-60)")
        .otherwise("Boomers (>60)"),
    )

    # enrichment
    logger.info("Joining tables...")

    df_enriched = (
        df_transfer.join(
            df_merchant, df_transfer.merchant_id == df_merchant.merch_id, "left"
        )
        .join(df_account, df_transfer.from_account_id == df_account.account_id, "left")
        .join(df_person_feat, df_account.owner_id == df_person_feat.person_id, "left")
    )

    # agg
    logger.info("Aggregating Consumption Data...")

    df_report = df_enriched.groupBy(
        col("txn_date").alias("report_date"),
        col("name").alias("merchant_name"),
        "category",
        col("city").alias("customer_city"),
        "gender",
        "age_group",
    ).agg(
        count("*").alias("total_transactions"), sum("amount").alias("total_spend_raw")
    )

    gold_utils.save_to_gold(
        df_report, "merchant_consumption_analysis", partition_cols=["report_date"]
    )

    spark.stop()


if __name__ == "__main__":
    run_merchant_mart()
