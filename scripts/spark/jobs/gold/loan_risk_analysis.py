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


def run_loan_risk_mart():
    logger.info("Starting Loan Risk & Analysis Mart...")
    spark = create_spark_session("safebank_gold_lending")

    # load data - the newest snapshot
    logger.info("Loading Silver Loan Data...")

    df_rate = (
        spark.read.format("delta")
        .load("s3a://silver/exchange_rate")
        .filter(col("effective_date") == current_date())
        .select("from_currency", "rate")
    )

    df_loan = (
        spark.read.format("delta")
        .load("s3a://silver/loan_account")
        .filter(col("is_current") == True)
        .select(
            col("id").alias("loan_id"),
            "account_id",
            "amount",
            "currency_code",
            "remaining_balance",
            "status",
            "interest_rate",
        )
    )

    df_account = (
        spark.read.format("delta")
        .load("s3a://silver/account")
        .filter(col("is_current") == True)
        .select(col("id").alias("account_id"), "branch_id")
    )

    df_branch = (
        spark.read.format("delta")
        .load("s3a://silver/branch")
        .select(col("id").alias("branch_id"), col("city").alias("branch_city"))
    )

    # enrichment
    logger.info("Enriching loan data & Converting currency...")

    df_enriched = df_loan.join(
        df_account, df_loan.account_id == df_account.account_id, "left"
    ).join(df_branch, df_account.branch_id == df_branch.branch_id, "left")

    df_with_rate = df_enriched.join(
        df_rate, df_enriched.currency_code == df_rate.from_currency, "left"
    )

    df_calculated = (
        df_with_rate.withColumn(
            "applied_rate",
            when(col("currency_code") == "VND", lit(1.0)).otherwise(
                coalesce(col("rate"), lit(1.0))
            ),
        )
        .withColumn("amount_vnd", col("amount") * col("applied_rate"))
        .withColumn(
            "remaining_balance",
            when(col("remaining_balance") < 0, 0).otherwise(col("remaining_balance")),
        )
        .withColumn(
            "remaining_balance_vnd", col("remaining_balance") * col("applied_rate")
        )
        .withColumn("paid_amount", col("amount") - col("remaining_balance"))
        .withColumn("paid_amount_vnd", col("amount_vnd") - col("remaining_balance_vnd"))
    )

    # risk metrics calculation
    logger.info("Calculating risk metrics...")

    df_risks = df_calculated.withColumn(
        "risk_category",
        when(col("status") == "DEFAULT", "High Risk")
        .when(col("status") == "CLOSED", "No Risk")
        .otherwise("Standard"),
    )

    # aggregation
    df_report = df_risks.groupBy(
        "branch_city", "currency_code", "status", "risk_category"
    ).agg(
        count("loan_id").alias("total_loans"),
        sum("amount").alias("total_funded_amount"),
        sum("remaining_balance").alias("total_outstanding_balance"),
        sum("paid_amount").alias("total_repaid_amount"),
        sum("amount_vnd").alias("total_funded_amount_vnd"),
        sum("remaining_balance_vnd").alias("total_outstanding_vnd"),
        sum("paid_amount_vnd").alias("total_repaid_amount_vnd"),
        avg("interest_rate").alias("avg_interest_rate"),
    )

    df_final = df_report.withColumn("snapshot_date", current_date())

    gold_utils.save_to_gold(
        df_final, "loan_risk_snapshot", partition_cols=["snapshot_date"]
    )

    spark.stop()


if __name__ == "__main__":
    run_loan_risk_mart()
