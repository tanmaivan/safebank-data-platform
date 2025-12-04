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


def run_security_mart():
    logger.info("Starting Security and Fraud Analysis Mart...")
    spark = create_spark_session("safebank_gold_security")

    logger.info("Loading Silver Data...")

    # Log
    df_sign_in = (
        spark.read.format("delta")
        .load("s3a://silver/sign_in")
        .withColumn("login_date", to_date(col("sign_in_time")))
        .select("account_id", "device_id", "location_city", "status", "login_date")
    )

    # Device
    df_device = (
        spark.read.format("delta")
        .load("s3a://silver/device")
        .select(col("id").alias("device_id"), "device_model", "is_trusted")
    )

    # Account and Person
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
        .select(col("id").alias("person_id"), col("city").alias("home_city"))
    )

    # enrichment
    logger.info("Analysing security risks...")

    df = (
        df_sign_in.join(df_device, df_sign_in.device_id == df_device.device_id, "left")
        .join(df_account, df_sign_in.account_id == df_account.account_id, "left")
        .join(df_person, df_account.owner_id == df_person.person_id, "left")
    )

    # flagging
    df_flagged = (
        df.withColumn(
            "is_failed_login", when(col("status") != "SUCCESS", 1).otherwise(0)
        )
        .withColumn("is_untrusted_device", when(col("is_trusted"), 1).otherwise(0))
        .withColumn(
            "is_strange_location",
            when(col("location_city") != col("home_city"), 1).otherwise(0),
        )
    )

    # aggregation
    logger.info("Aggregating daily security report...")

    df_report = df_flagged.groupBy(
        col("login_date").alias("report_date"), "location_city", "device_model"
    ).agg(
        count("*").alias("total_login"),
        sum("is_failed_login").alias("total_failed_logins"),
        sum("is_untrusted_device").alias("total_untrusted_device_logins"),
        sum("is_strange_location").alias("total_strange_loc_logins"),
    )

    # write to gold
    gold_utils.save_to_gold(
        df_report, "daily_security_summary", partition_cols=["report_date"]
    )

    spark.stop()


if __name__ == "__main__":
    run_security_mart()
