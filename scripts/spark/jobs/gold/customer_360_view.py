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


def run_customer_360():
    logger.info("Starting Customer 360 View Construction...")
    spark = create_spark_session("safebank_gold_cust360")

    logger.info("Loading Silver Data...")

    df_rate = (
        spark.read.format("delta")
        .load("s3a://silver/exchange_rate")
        .filter(col("effective_date") == current_date())
        .select("from_currency", "rate")
    )

    df_person = (
        spark.read.format("delta")
        .load("s3a://silver/person")
        .filter(col("is_current") == True)
        .select(
            col("id").alias("person_id"),
            "name",
            "gender",
            "birthday",
            "city",
            "is_blocked",
        )
    )

    df_account = (
        spark.read.format("delta")
        .load("s3a://silver/account")
        .filter(col("is_current") == True)
        .select(col("id").alias("account_id"), "owner_id", "balance", "currency_code")
    )

    df_loan = (
        spark.read.format("delta")
        .load("s3a://silver/loan_account")
        .filter(col("is_current") == True)
        .select("account_id", "remaining_balance", "currency_code")
    )

    df_signin = (
        spark.read.format("delta")
        .load("s3a://silver/sign_in")
        .select("account_id", "sign_in_time")
    )

    # pre-process
    logger.info("Converting Account & Loan to VND...")

    # - balance vnd
    df_account_vnd = (
        df_account.join(
            df_rate, df_account.currency_code == df_rate.from_currency, "left"
        )
        .withColumn(
            "applied_rate",
            when(col("currency_code") == "VND", lit(1.0)).otherwise(
                coalesce(col("rate"), lit(1.0))
            ),
        )
        .withColumn("balance_vnd", col("balance") * col("applied_rate"))
        .select("account_id", "owner_id", "balance_vnd")
    )

    df_loan_vnd = (
        df_loan.join(df_rate, df_loan.currency_code == df_rate.from_currency, "left")
        .withColumn(
            "applied_rate",
            when(col("currency_code") == "VND", lit(1.0)).otherwise(
                coalesce(col("rate"), lit(1.0))
            ),
        )
        .withColumn("debt_vnd", col("remaining_balance") * col("applied_rate"))
        .select("account_id", "debt_vnd")
    )

    # aggregate components
    logger.info("Aggregating Components...")

    df_assets = df_account_vnd.groupBy("owner_id").agg(
        sum("balance_vnd").alias("total_balance_vnd"),
        count("account_id").alias("num_accounts"),
    )

    df_liabilities = (
        df_loan_vnd.join(
            df_account, df_loan_vnd.account_id == df_account.account_id, "inner"
        )
        .groupBy("owner_id")
        .agg(
            sum("debt_vnd").alias("total_debt_vnd"),
            count(df_loan_vnd.account_id).alias("num_loans"),
        )
    )

    df_behavior = (
        df_signin.join(
            df_account, df_signin.account_id == df_account.account_id, "inner"
        )
        .groupBy("owner_id")
        .agg(
            max("sign_in_time").alias("last_login_ts"),
            count(df_signin.sign_in_time).alias("login_count_lifetime"),
        )
    )

    # 360 view
    logger.info("Executing the big join...")

    df_360 = (
        df_person.join(df_assets, df_person.person_id == df_assets.owner_id, "left")
        .drop("owner_id")
        .join(df_liabilities, df_person.person_id == df_liabilities.owner_id, "left")
        .drop("owner_id")
        .join(df_behavior, df_person.person_id == df_behavior.owner_id, "left")
        .drop("owner_id")
    )

    # enrichment
    df_final = (
        df_360.withColumn(
            "total_balance_vnd", coalesce(col("total_balance_vnd"), lit(0))
        )
        .withColumn("total_debt_vnd", coalesce(col("total_debt_vnd"), lit(0)))
        .withColumn("age", year(current_date()) - year(col("birthday")))
        .withColumn("net_worth", col("total_balance_vnd") - col("total_debt_vnd"))
    )

    # customer segmentation
    df_segmented = df_final.withColumn(
        "segment",
        when(col("is_blocked") == True, "Blocked")
        .when(col("net_worth") > 1000000000, "VIP Diamond")
        .when(col("net_worth") > 100000000, "VIP Gold")
        .when(col("total_debt_vnd") > col("total_balance_vnd"), "High Debt")
        .otherwise("Standard"),
    )

    df_output = df_segmented.withColumn("report_date", current_date())

    # write to gold
    gold_utils.save_to_gold(df_output, "customer_360", partition_cols=["report_date"])

    spark.stop()


if __name__ == "__main__":
    run_customer_360()
